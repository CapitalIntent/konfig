//! Client-identity extraction from the presented mTLS leaf certificate.
//!
//! The gRPC server requires client auth (see [`super::tls`]); every accepted
//! connection therefore carries at least one peer certificate. This module
//! derives a stable, human-readable identity string from that leaf cert for
//! the audit log (CU-86ahrwd6h) and the later authz layer.
//!
//! Resolution order (locked by design):
//!   1. First Subject Alternative Name of type **URI** (e.g. a SPIFFE id
//!      `spiffe://trust-domain/workload`). SVIDs encode identity in the SAN
//!      URI, so it is preferred over the Subject DN.
//!   2. Subject **Common Name** (CN) when no SAN URI is present.
//!   3. Literal `"anonymous"` when no cert is presented or parsing fails.
//!
//! No SPIFFE Workload API / SVID validation here — we read whatever the TLS
//! stack already authenticated against the configured client CA.

use tonic::Request;
use x509_parser::prelude::*;

/// Identity derived from the peer's mTLS leaf certificate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientIdentity {
    /// SAN URI, Subject CN, or `"anonymous"`.
    pub id: String,
    /// `true` when no usable identity could be derived (no cert / parse
    /// failure / empty SAN+CN) and `id` fell back to `"anonymous"`.
    pub anonymous: bool,
}

/// The literal identity used when no client identity can be derived.
pub const ANONYMOUS: &str = "anonymous";

impl ClientIdentity {
    fn anonymous() -> Self {
        Self {
            id: ANONYMOUS.to_string(),
            anonymous: true,
        }
    }

    fn named(id: String) -> Self {
        Self {
            id,
            anonymous: false,
        }
    }
}

/// Extract the [`ClientIdentity`] from an inbound request's peer certificates.
///
/// `tonic::Request::peer_certs` returns `Option<Arc<Vec<CertificateDer>>>`;
/// the client leaf is the first entry (RFC 5246 orders the Certificate message
/// leaf-first). Absent cert or any parse failure degrades to anonymous rather
/// than erroring — identity is advisory metadata, never a request gate here.
pub fn extract_identity<T>(req: &Request<T>) -> ClientIdentity {
    let Some(certs) = req.peer_certs() else {
        return ClientIdentity::anonymous();
    };
    let Some(leaf) = certs.first() else {
        return ClientIdentity::anonymous();
    };
    identity_from_der(leaf.as_ref())
}

/// Derive the identity string from a single DER-encoded leaf certificate.
///
/// Split out from [`extract_identity`] so the SAN/CN resolution is unit-
/// testable from raw DER without a live TLS connection.
pub fn identity_from_der(der: &[u8]) -> ClientIdentity {
    let Ok((_, cert)) = parse_x509_certificate(der) else {
        return ClientIdentity::anonymous();
    };

    // 1. First SAN URI.
    if let Ok(Some(san)) = cert.subject_alternative_name() {
        for name in &san.value.general_names {
            if let GeneralName::URI(uri) = name {
                let uri = uri.trim();
                if !uri.is_empty() {
                    return ClientIdentity::named(uri.to_string());
                }
            }
        }
    }

    // 2. Subject CN.
    if let Some(cn) = cert.subject().iter_common_name().next()
        && let Ok(cn) = cn.as_str()
    {
        let cn = cn.trim();
        if !cn.is_empty() {
            return ClientIdentity::named(cn.to_string());
        }
    }

    // 3. Fallback.
    ClientIdentity::anonymous()
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;

    // Self-signed EC test certs (validity 100y). NOT production keys — only
    // used to exercise SAN-URI vs CN resolution. Generated with
    // `openssl ecparam -name prime256v1` + `openssl req -x509`.

    // Subject CN = `san-cert-cn`, SAN URI = `spiffe://konfig/client/test`.
    // The CN differs from the SAN URI so the SAN-first preference is provable.
    const SAN_URI_CERT_PEM: &str = "-----BEGIN CERTIFICATE-----\n\
MIIBejCCAR+gAwIBAgIUF8OoPO3SQ0iC7VpkKf+RWRW9gcUwCgYIKoZIzj0EAwIw\n\
FjEUMBIGA1UEAwwLc2FuLWNlcnQtY24wIBcNMjYwNjIyMTkwNTI4WhgPMjEyNjA1\n\
MjkxOTA1MjhaMBYxFDASBgNVBAMMC3Nhbi1jZXJ0LWNuMFkwEwYHKoZIzj0CAQYI\n\
KoZIzj0DAQcDQgAEnHHxILp/1w6x2DQqz4+uZmxV+zeQ/+F1IjcRNiQ4yzfHR7fa\n\
SYfCUG8HQNdMkVNhqwqZnT2mf7ydkZUayM5876NJMEcwJgYDVR0RBB8wHYYbc3Bp\n\
ZmZlOi8va29uZmlnL2NsaWVudC90ZXN0MB0GA1UdDgQWBBTKh2kTQfA8qzieqev1\n\
j+s3Cv85RTAKBggqhkjOPQQDAgNJADBGAiEAm5SxcZdPanSVtLpYt039kMAKv7VF\n\
N3vDVig1iADfyn8CIQCHTppgn3fqWdAfBSIW7630qZ2Zs6esZytkNZyNnDP3rw==\n\
-----END CERTIFICATE-----\n";

    // Subject CN = `konfig-client-cn`, no SAN extension.
    const CN_ONLY_CERT_PEM: &str = "-----BEGIN CERTIFICATE-----\n\
MIIBjTCCATOgAwIBAgIUJTkzQWK9zsBpXHSvw/ApcjkF80UwCgYIKoZIzj0EAwIw\n\
GzEZMBcGA1UEAwwQa29uZmlnLWNsaWVudC1jbjAgFw0yNjA2MjIxOTA1MjhaGA8y\n\
MTI2MDUyOTE5MDUyOFowGzEZMBcGA1UEAwwQa29uZmlnLWNsaWVudC1jbjBZMBMG\n\
ByqGSM49AgEGCCqGSM49AwEHA0IABIR9ewyVxM1p6l5EsdNc2ZpGLnqGDIxD6r6I\n\
5cIMrqhsgDJjp8XdrsDQi5f1K8drmzJ7MenF6wNO0QptvIN1+uCjUzBRMB0GA1Ud\n\
DgQWBBR7AIyaIHiSy7skw+J6WRntTeKngTAfBgNVHSMEGDAWgBR7AIyaIHiSy7sk\n\
w+J6WRntTeKngTAPBgNVHRMBAf8EBTADAQH/MAoGCCqGSM49BAMCA0gAMEUCIQDr\n\
BYujxFdgzi2sCWkTvVutoXd1Y4tMB3adIIhutvsdKgIgVbD7hpfujJNKzsjh138n\n\
EmWrpZG3E8j4+gncMGW52z4=\n\
-----END CERTIFICATE-----\n";

    /// Decode a single-cert PEM string to DER (strip header/footer + base64).
    fn pem_to_der(pem: &str) -> Vec<u8> {
        let b64: String = pem
            .lines()
            .filter(|l| !l.starts_with("-----"))
            .collect::<Vec<_>>()
            .join("");
        base64::engine::general_purpose::STANDARD
            .decode(b64.as_bytes())
            .expect("valid base64 PEM body")
    }

    /// A cert carrying a SAN URI resolves to that URI even though it also has
    /// a Subject CN — SAN URI wins.
    #[test]
    fn san_uri_preferred_over_cn() {
        let der = pem_to_der(SAN_URI_CERT_PEM);
        let id = identity_from_der(&der);
        assert_eq!(id.id, "spiffe://konfig/client/test");
        assert!(!id.anonymous);
    }

    /// A cert with only a Subject CN (no SAN) resolves to the CN.
    #[test]
    fn cn_used_when_no_san_uri() {
        let der = pem_to_der(CN_ONLY_CERT_PEM);
        let id = identity_from_der(&der);
        assert_eq!(id.id, "konfig-client-cn");
        assert!(!id.anonymous);
    }

    /// Unparseable / garbage DER degrades to anonymous rather than panicking.
    #[test]
    fn garbage_der_is_anonymous() {
        let id = identity_from_der(b"not a certificate");
        assert_eq!(id.id, ANONYMOUS);
        assert!(id.anonymous);
    }

    /// An in-process request carries no peer certs, so `extract_identity`
    /// returns anonymous (mirrors the `client_addr` "unknown" fallback).
    #[test]
    fn no_peer_cert_is_anonymous() {
        let req = Request::new(());
        let id = extract_identity(&req);
        assert_eq!(id.id, ANONYMOUS);
        assert!(id.anonymous);
    }
}
