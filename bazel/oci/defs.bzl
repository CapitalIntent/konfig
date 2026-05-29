"""Shared macro for konfig container images.

`konfig_oci_image` wraps the same pipeline that each `docker/<name>/BUILD.bazel`
used to repeat by hand: cross-compile binary → rename into a layer (0755 perms)
→ `oci_image` on distroless/cc/arm64 → load + push (sha + latest tags).

Keeps the three image packages (`konfig`, `konfig-cli`, `konfig-loadtest`) in
lock-step so a single edit here propagates to all of them.
"""

load("@aspect_bazel_lib//lib:expand_template.bzl", "expand_template")
load("@rules_oci//oci:defs.bzl", "oci_image", "oci_load", "oci_push")
load("@rules_pkg//pkg:mappings.bzl", "pkg_attributes", "pkg_files", "strip_prefix")
load("@rules_pkg//pkg:tar.bzl", "pkg_tar")
load("//bazel/oci:transitions.bzl", "platform_transition_binary")

def konfig_oci_image(
        name,
        binary,
        binary_name,
        repository,
        exposed_ports = None,
        platform = "//platforms:linux_arm64",
        base = "@distroless_cc_linux_arm64_v8"):
    """Build/load/push a Konfig container image.

    Args:
      name: package-unique prefix; produces `:image`, `:load`, `:push`.
      binary: label of a rust_binary to package (host-platform target).
      binary_name: filename written into / inside the image (also the entrypoint).
      repository: Docker Hub repository (e.g. "kasa288/konfig").
      exposed_ports: optional list like ["50051/tcp", "9090/tcp"].
      platform: target platform label; defaults to linux/arm64.
      base: oci_image base; defaults to distroless/cc linux-arm64.
    """
    transitioned = "_{}_bin_transitioned".format(name)
    files_target = "_{}_files".format(name)
    layer_target = "_{}_layer".format(name)
    tags_target = "_{}_remote_tags".format(name)

    platform_transition_binary(
        name = transitioned,
        binary = binary,
        platform = platform,
    )

    pkg_files(
        name = files_target,
        srcs = [":" + transitioned],
        attributes = pkg_attributes(mode = "0755"),
        prefix = "/",
        renames = {":" + transitioned: binary_name},
        strip_prefix = strip_prefix.from_root(),
    )

    pkg_tar(
        name = layer_target,
        srcs = [":" + files_target],
    )

    oci_image(
        name = "image",
        base = base,
        entrypoint = ["/" + binary_name],
        exposed_ports = exposed_ports or [],
        tars = [":" + layer_target],
    )

    # Stamped tag list: short git SHA + "latest". The literal "0000000" gets
    # substituted with STABLE_GIT_SHA only when `bazel run --stamp` is used; the
    # placeholder keeps non-stamped builds deterministic and cacheable.
    expand_template(
        name = tags_target,
        out = "_{}_remote_tags.txt".format(name),
        stamp_substitutions = {"0000000": "{{STABLE_GIT_SHA}}"},
        template = [
            "0000000",
            "latest",
        ],
    )

    oci_load(
        name = "load",
        image = ":image",
        repo_tags = ["{}:latest".format(repository)],
    )

    oci_push(
        name = "push",
        image = ":image",
        remote_tags = ":" + tags_target,
        repository = repository,
    )
