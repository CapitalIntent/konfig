//! konfig-cli — operator CLI for konfig.
//!
//! Reads and writes Config CRDs directly via kube-rs, bypassing the gRPC
//! service. Works even when the konfig server is down.
//!
//! # Commands
//!
//! - `apply <namespace> <name> <yaml-file>` — create/update a Config CRD
//! - `dry-run <namespace> <name> <yaml-file>` — preview an apply (no write)
//! - `get <namespace> <name>` — print a Config CRD spec as YAML
//! - `revert <namespace> <name> <resource-version>` — roll back to a historical RV
//! - `import configmap <namespace> <name> [--target <name>]` — import an existing ConfigMap
//! - `get-secret <namespace> <name> [--reveal]` — print secret keys (values redacted)
//! - `apply-secret <namespace> <name> <yaml-file>` — patch a managed Secret

use std::collections::BTreeMap;
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use k8s_openapi::ByteString;
use k8s_openapi::api::core::v1::Secret;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::Client;
use kube::api::{Api, Patch, PatchParams};

#[derive(Parser)]
#[command(name = "konfig-cli", about = "Konfig operator CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Create or update a Config CRD from a YAML file.
    Apply {
        namespace: String,
        name: String,
        yaml_file: PathBuf,
    },
    /// Preview what an apply would change WITHOUT writing to K8s.
    ///
    /// Runs the same schema_version monotonicity gate as `apply` and prints the
    /// current vs proposed content/version. Never patches the cluster.
    DryRun {
        namespace: String,
        name: String,
        yaml_file: PathBuf,
    },
    /// Print a Config CRD spec as YAML.
    Get { namespace: String, name: String },
    /// Roll back a Config to a historical resourceVersion.
    ///
    /// Looks up the historical content directly via the K8s API (server-side,
    /// resourceVersionMatch=Exact), bumps the schema_version above the current
    /// value to preserve monotonicity, and patches the CRD with the historical
    /// content under the new version.
    Revert {
        namespace: String,
        name: String,
        /// The historical resourceVersion to roll back to.
        resource_version: String,
    },
    /// Onboard existing K8s objects as Config CRDs.
    Import {
        #[command(subcommand)]
        source: ImportSource,
    },
    /// Get a Secret managed by Konfig. Values are redacted by default.
    GetSecret {
        namespace: String,
        name: String,
        /// Print decoded (plaintext) values instead of redacting them.
        #[arg(long)]
        reveal: bool,
    },
    /// Patch a K8s Secret with key→value pairs from a YAML file.
    ///
    /// The YAML file must contain a `schema_version` key plus data keys.
    /// Values are base64-encoded before patching.
    ApplySecret {
        namespace: String,
        name: String,
        yaml_file: PathBuf,
    },
}

#[derive(Subcommand)]
enum ImportSource {
    /// Import a ConfigMap's data field as a Config CRD.
    Configmap {
        namespace: String,
        /// ConfigMap name to read.
        name: String,
        /// Target Config name (defaults to same as configmap name).
        #[arg(long)]
        target: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("konfig_cli=info".parse()?),
        )
        .init();

    let cli = Cli::parse();
    let client = Client::try_default().await?;

    match cli.command {
        Commands::Apply {
            namespace,
            name,
            yaml_file,
        } => {
            cmd_apply(client, &namespace, &name, &yaml_file).await?;
        }
        Commands::DryRun {
            namespace,
            name,
            yaml_file,
        } => {
            cmd_dry_run(client, &namespace, &name, &yaml_file).await?;
        }
        Commands::Get { namespace, name } => {
            cmd_get(client, &namespace, &name).await?;
        }
        Commands::Revert {
            namespace,
            name,
            resource_version,
        } => {
            cmd_revert(client, &namespace, &name, &resource_version).await?;
        }
        Commands::Import {
            source:
                ImportSource::Configmap {
                    namespace,
                    name,
                    target,
                },
        } => {
            let target_name = target.as_deref().unwrap_or(&name);
            cmd_import_configmap(client, &namespace, &name, target_name).await?;
        }
        Commands::GetSecret {
            namespace,
            name,
            reveal,
        } => {
            cmd_get_secret(client, &namespace, &name, reveal).await?;
        }
        Commands::ApplySecret {
            namespace,
            name,
            yaml_file,
        } => {
            let yaml_content = std::fs::read_to_string(&yaml_file)
                .map_err(|e| format!("cannot read {}: {e}", yaml_file.display()))?;
            cmd_apply_secret(client, &namespace, &name, &yaml_content).await?;
        }
    }

    Ok(())
}

async fn cmd_apply(
    client: Client,
    namespace: &str,
    name: &str,
    yaml_file: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let yaml_content = std::fs::read_to_string(yaml_file)?;
    // The CLI applies directly (no server-side watcher / schema registry), so
    // it passes an empty `SchemaTable` — no schema for any key ⇒ accept
    // anything, preserving the CLI's historical behaviour. Server-side schema
    // validation (CU-86ahrwd5g) is enforced by the konfig server's Apply RPC.
    let schema_table = std::sync::Arc::new(konfig::schema::SchemaTable::new());
    let result =
        konfig::grpc::apply::apply_inner(namespace, name, &yaml_content, client, &schema_table)
            .await
            .map_err(|s| format!("Apply failed: {s}"))?;
    let rv = result.into_inner().resource_version;
    println!("Applied {namespace}/{name} (resource_version: {rv})");
    Ok(())
}

async fn cmd_dry_run(
    client: Client,
    namespace: &str,
    name: &str,
    yaml_file: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    use konfig::proto::DryRunApplyRequest;

    let yaml_content = std::fs::read_to_string(yaml_file)?;
    let req = DryRunApplyRequest {
        namespace: namespace.to_string(),
        name: name.to_string(),
        yaml_content,
    };
    let result = konfig::grpc::apply::handle_dry_run_apply(client, req)
        .await
        .map_err(|s| format!("DryRunApply failed: {s}"))?;
    let resp = result.into_inner();

    println!("# Dry-run diff for {namespace}/{name} (NO changes written)");
    println!(
        "schema_version: {} -> {}",
        resp.current_schema_version, resp.proposed_schema_version
    );
    println!("--- current content");
    println!(
        "{}",
        if resp.current_content_json.is_empty() {
            "<absent>"
        } else {
            &resp.current_content_json
        }
    );
    println!("+++ proposed content");
    println!("{}", resp.proposed_content_json);
    Ok(())
}

async fn cmd_revert(
    client: Client,
    namespace: &str,
    name: &str,
    resource_version: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use konfig::proto::RevertRequest;

    let req = RevertRequest {
        namespace: namespace.to_string(),
        name: name.to_string(),
        to_resource_version: resource_version.to_string(),
    };
    let result = konfig::grpc::revert::handle_revert(client, req)
        .await
        .map_err(|s| format!("Revert failed: {s}"))?;
    let resp = result.into_inner();
    println!(
        "Reverted {namespace}/{name} to resource_version={resource_version} \
         (new schema_version: {}, new resource_version: {})",
        resp.schema_version, resp.resource_version
    );
    Ok(())
}

async fn cmd_get(
    client: Client,
    namespace: &str,
    name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use konfig::watcher::config_api_resource;
    use kube::api::Api;
    use kube::core::DynamicObject;

    let ar = config_api_resource();
    let api: Api<DynamicObject> = Api::namespaced_with(client, namespace, &ar);

    match api.get(name).await {
        Ok(obj) => {
            let spec = obj
                .data
                .get("spec")
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            let yaml = serde_yaml::to_string(&spec)?;
            println!("{yaml}");
        }
        Err(kube::Error::Api(ref ae)) if ae.code == 404 => {
            eprintln!("Config {namespace}/{name} not found");
            std::process::exit(1);
        }
        Err(e) => return Err(e.into()),
    }

    Ok(())
}

async fn cmd_import_configmap(
    client: Client,
    namespace: &str,
    configmap_name: &str,
    target_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let result =
        konfig::import::import_configmap(client, namespace, configmap_name, target_name).await?;
    println!(
        "Imported {namespace}/{configmap_name} → Config {namespace}/{target_name} (rv: {})",
        result.resource_version
    );
    Ok(())
}

async fn cmd_get_secret(
    client: Client,
    namespace: &str,
    name: &str,
    reveal: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let secrets: Api<Secret> = Api::namespaced(client, namespace);
    let secret = secrets
        .get(name)
        .await
        .map_err(|e| format!("kube get error: {e}"))?;

    let schema_version: u32 = secret
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get("konfig.io/schema-version"))
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    println!("namespace: {namespace}");
    println!("name: {name}");
    println!("schema_version: {schema_version}");

    if let Some(data) = &secret.data {
        for (k, ByteString(raw_bytes)) in data {
            if reveal {
                let value = std::str::from_utf8(raw_bytes).unwrap_or("<binary>");
                println!("{k}: {value}");
            } else {
                println!("{k}: <REDACTED>");
            }
        }
    } else {
        println!("# No data keys.");
    }

    Ok(())
}

async fn cmd_apply_secret(
    client: Client,
    namespace: &str,
    name: &str,
    yaml_content: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use base64::Engine;

    let plaintext_map: BTreeMap<String, String> =
        serde_yaml::from_str(yaml_content).map_err(|e| format!("invalid YAML: {e}"))?;

    let incoming_version: u32 = plaintext_map
        .get("schema_version")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    // Check monotonicity.
    let secrets: Api<Secret> = Api::namespaced(client, namespace);
    let current_version: u32 = match secrets.get(name).await {
        Ok(s) => s
            .metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get("konfig.io/schema-version"))
            .and_then(|v| v.parse().ok())
            .unwrap_or(0),
        Err(kube::Error::Api(ref ae)) if ae.code == 404 => 0,
        Err(e) => return Err(format!("kube get error: {e}").into()),
    };

    if incoming_version <= current_version {
        return Err(format!(
            "schema_version must be > current ({current_version}); got {incoming_version}"
        )
        .into());
    }

    let encoded_data: BTreeMap<String, ByteString> = plaintext_map
        .iter()
        .filter(|(k, _)| k.as_str() != "schema_version")
        .map(|(k, v)| {
            let b64 = base64::engine::general_purpose::STANDARD.encode(v.as_bytes());
            (k.clone(), ByteString(b64.into_bytes()))
        })
        .collect();

    let patch = Secret {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            labels: Some({
                let mut l = BTreeMap::new();
                l.insert("konfig.io/managed".to_string(), "true".to_string());
                l
            }),
            annotations: Some({
                let mut a = BTreeMap::new();
                a.insert(
                    "konfig.io/schema-version".to_string(),
                    incoming_version.to_string(),
                );
                a
            }),
            ..Default::default()
        },
        data: if encoded_data.is_empty() {
            None
        } else {
            Some(encoded_data)
        },
        ..Default::default()
    };

    let ssapply = PatchParams::apply("konfig.v1").force();
    let result = secrets
        .patch(name, &ssapply, &Patch::Apply(&patch))
        .await
        .map_err(|e| format!("kube patch error: {e}"))?;

    let rv = result.metadata.resource_version.unwrap_or_default();
    println!("Applied {namespace}/{name} (resource_version: {rv})");
    Ok(())
}
