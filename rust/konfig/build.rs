fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Request messages the HTTP/JSON gateway (CU-86ahrwd70) deserializes from a
    // JSON request body. `#[serde(default)]` gives them proto3-JSON leniency:
    // an omitted field deserializes to the proto default instead of erroring.
    // Scoped to messages ONLY — a container-level `#[serde(default)]` on the
    // generated `*EventType` enums does not compile.
    const JSON_REQUEST_MESSAGES: [&str; 9] = [
        "konfig.v1.GetRequest",
        "konfig.v1.GetAllRequest",
        "konfig.v1.ApplyRequest",
        "konfig.v1.BatchApplyRequest",
        "konfig.v1.DryRunApplyRequest",
        "konfig.v1.RevertRequest",
        "konfig.v1.GetSecretRequest",
        "konfig.v1.GetAllSecretsRequest",
        "konfig.v1.ApplySecretRequest",
    ];

    let mut builder = tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        // serde on every generated message + enum so the HTTP/JSON gateway
        // (CU-86ahrwd70) can (de)serialize the request/response types directly.
        // The JSON shape is prost's snake_case field naming, which matches the
        // wire contract (e.g. `{"namespace":"...","name":"..."}`).
        .type_attribute(".", "#[derive(::serde::Serialize, ::serde::Deserialize)]");

    for msg in JSON_REQUEST_MESSAGES {
        builder = builder.type_attribute(msg, "#[serde(default)]");
    }

    builder.compile_protos(
        &["../../proto/konfig/v1/konfig_service.proto"],
        &["../../proto"],
    )?;
    Ok(())
}
