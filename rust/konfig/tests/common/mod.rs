//! Shared helpers for konfig K8s integration tests.

#![cfg(feature = "integration")]

use std::env;
use std::time::Duration;

use kube::api::{Api, ApiResource, DeleteParams, Patch, PatchParams, PostParams};
use kube::core::DynamicObject;
use serde_json::json;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::k3s::{K3s, KUBE_SECURE_PORT};

use konfig::watcher::config_api_resource;

// ── K3s harness ───────────────────────────────────────────────────────────────

pub async fn k3s_client() -> (ContainerAsync<K3s>, kube::Client) {
    let conf_dir = env::temp_dir().join(format!("k3s-konfig-{}", uuid_simple()));
    std::fs::create_dir_all(&conf_dir).expect("create k3s conf dir");

    let container = K3s::default()
        .with_conf_mount(&conf_dir)
        .with_privileged(true)
        .with_userns_mode("host")
        .start()
        .await
        .expect("start K3s container — is Docker running?");

    let _ = rustls::crypto::ring::default_provider().install_default();

    let kubeconfig_yaml = container.image().read_kube_config().expect("read kubeconfig");
    let mut kube_config =
        kube::config::Kubeconfig::from_yaml(&kubeconfig_yaml).expect("parse kubeconfig");

    let host_port = container.get_host_port_ipv4(KUBE_SECURE_PORT).await.expect("K3s host port");

    kube_config.clusters.iter_mut().for_each(|named| {
        if let Some(c) = named.cluster.as_mut() {
            if let Some(s) = c.server.as_mut() {
                *s = format!("https://127.0.0.1:{host_port}");
            }
        }
    });

    let config = kube::Config::from_custom_kubeconfig(
        kube_config,
        &kube::config::KubeConfigOptions::default(),
    )
    .await
    .expect("build kube Config");

    let client = kube::Client::try_from(config).expect("create kube Client");
    (container, client)
}

// ── CRD installation ──────────────────────────────────────────────────────────

/// Load `infra/konfig/crd.yaml` at runtime via Bazel runfiles or CARGO_MANIFEST_DIR.
fn load_crd_yaml() -> String {
    // Bazel path: _main/infra/konfig/crd.yaml
    // Cargo path: <workspace_root>/infra/konfig/crd.yaml
    const BAZEL_REL: &str = "_main/infra/konfig/crd.yaml";
    const CARGO_REL: &str = "infra/konfig/crd.yaml";

    std::env::var("RUNFILES_DIR")
        .ok()
        .or_else(|| std::env::var("TEST_SRCDIR").ok())
        .and_then(|dir| std::fs::read_to_string(std::path::Path::new(&dir).join(BAZEL_REL)).ok())
        .or_else(|| {
            // cargo test: CARGO_MANIFEST_DIR is rust/konfig — workspace root is 2 levels up.
            std::env::var("CARGO_MANIFEST_DIR").ok().and_then(|manifest| {
                let workspace = std::path::Path::new(&manifest).ancestors().nth(2)?.to_path_buf();
                std::fs::read_to_string(workspace.join(CARGO_REL)).ok()
            })
        })
        .expect("infra/konfig/crd.yaml not found — run via bazel test or cargo test from repo root")
}

/// Apply the `configs.konfig.io` CRD to the cluster using a DynamicObject,
/// then wait 2s for it to be established.
pub async fn install_crd(client: &kube::Client) {
    let crd_ar = ApiResource {
        group: "apiextensions.k8s.io".to_string(),
        version: "v1".to_string(),
        api_version: "apiextensions.k8s.io/v1".to_string(),
        kind: "CustomResourceDefinition".to_string(),
        plural: "customresourcedefinitions".to_string(),
    };

    let crd_yaml = load_crd_yaml();
    let crd_value: serde_json::Value = serde_yaml::from_str(&crd_yaml).expect("parse CRD YAML");

    let api: Api<DynamicObject> = Api::all_with(client.clone(), &crd_ar);
    let pp = PatchParams::apply("konfig-test").force();
    api.patch("configs.konfig.io", &pp, &Patch::Apply(crd_value)).await.expect("apply Config CRD");

    tokio::time::sleep(Duration::from_secs(2)).await;
}

// ── Config CRD helpers ────────────────────────────────────────────────────────

pub async fn upsert_config(
    client: &kube::Client,
    namespace: &str,
    name: &str,
    schema_version: u32,
    content: serde_json::Value,
) -> Result<(), kube::Error> {
    let ar = config_api_resource();
    let api: Api<DynamicObject> = Api::namespaced_with(client.clone(), namespace, &ar);

    let body = json!({
        "apiVersion": "konfig.io/v1",
        "kind": "Config",
        "metadata": { "name": name, "namespace": namespace },
        "spec": { "schema_version": schema_version, "content": content }
    });

    match api.create(&PostParams::default(), &serde_json::from_value(body.clone()).unwrap()).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(ref ae)) if ae.code == 409 => {
            maybe_delete(client, namespace, name).await;
            tokio::time::sleep(Duration::from_millis(300)).await;
            let obj: DynamicObject = serde_json::from_value(body).unwrap();
            api.create(&PostParams::default(), &obj).await?;
            Ok(())
        }
        Err(e) => Err(e),
    }
}

pub async fn maybe_delete(client: &kube::Client, namespace: &str, name: &str) {
    let ar = config_api_resource();
    let api: Api<DynamicObject> = Api::namespaced_with(client.clone(), namespace, &ar);
    let _ = api.delete(name, &DeleteParams::default()).await;
}

pub async fn poll_until<F>(deadline: Duration, interval: Duration, mut predicate: F)
where
    F: FnMut() -> bool,
{
    let start = tokio::time::Instant::now();
    loop {
        if predicate() {
            return;
        }
        if start.elapsed() >= deadline {
            panic!("poll_until: condition not satisfied within {deadline:?}");
        }
        tokio::time::sleep(interval).await;
    }
}

pub fn uuid_simple() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().subsec_nanos();
    format!("{nanos:08x}")
}
