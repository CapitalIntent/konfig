//! konfig-cli — operator CLI for konfig.
//!
//! Reads and writes Config CRDs directly via kube-rs, bypassing the gRPC
//! service. Works even when the konfig server is down.
//!
//! # Commands
//!
//! - `apply <namespace> <name> <yaml-file>` — create/update a Config CRD
//! - `get <namespace> <name>` — print a Config CRD spec as YAML
//! - `import configmap <namespace> <name> [--target <name>]` — import an existing ConfigMap

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use kube::Client;

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
    /// Print a Config CRD spec as YAML.
    Get {
        namespace: String,
        name: String,
    },
    /// Onboard existing K8s objects as Config CRDs.
    Import {
        #[command(subcommand)]
        source: ImportSource,
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
        Commands::Apply { namespace, name, yaml_file } => {
            cmd_apply(client, &namespace, &name, &yaml_file).await?;
        }
        Commands::Get { namespace, name } => {
            cmd_get(client, &namespace, &name).await?;
        }
        Commands::Import { source: ImportSource::Configmap { namespace, name, target } } => {
            let target_name = target.as_deref().unwrap_or(&name);
            cmd_import_configmap(client, &namespace, &name, target_name).await?;
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
    let result = konfig::grpc::apply::apply_inner(namespace, name, &yaml_content, client).await
        .map_err(|s| format!("Apply failed: {s}"))?;
    let rv = result.into_inner().resource_version;
    println!("Applied {namespace}/{name} (resource_version: {rv})");
    Ok(())
}

async fn cmd_get(
    client: Client,
    namespace: &str,
    name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use kube::api::Api;
    use kube::core::DynamicObject;
    use konfig::watcher::config_api_resource;

    let ar = config_api_resource();
    let api: Api<DynamicObject> = Api::namespaced_with(client, namespace, &ar);

    match api.get(name).await {
        Ok(obj) => {
            let spec = obj.data.get("spec").cloned().unwrap_or(serde_json::Value::Null);
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
    let result = konfig::import::import_configmap(client, namespace, configmap_name, target_name).await?;
    println!("Imported {namespace}/{configmap_name} → Config {namespace}/{target_name} (rv: {})", result.resource_version);
    Ok(())
}
