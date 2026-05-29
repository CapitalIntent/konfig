"""Shared macro for konfig container images.

`konfig_oci_image` packages a Rust binary into a multi-arch OCI image:
per-arch oci_image (linux/amd64 + linux/arm64) -> oci_image_index ->
load + push (sha + latest tags).

Each per-arch image runs the binary through `platform_transition_binary`,
which flips the platform AND release-config rustc flags (compilation_mode=
opt, -Cstrip=symbols, -Cpanic=abort). The host-config tools (bsd_tar,
oci_load runner) stay on the host platform.

Keeps the three image packages (`konfig`, `konfig-cli`, `konfig-loadtest`)
in lock-step so a single edit here propagates to all of them.
"""

load("@aspect_bazel_lib//lib:expand_template.bzl", "expand_template")
load("@rules_oci//oci:defs.bzl", "oci_image", "oci_image_index", "oci_load", "oci_push")
load("@rules_pkg//pkg:mappings.bzl", "pkg_attributes", "pkg_files", "strip_prefix")
load("@rules_pkg//pkg:tar.bzl", "pkg_tar")
load("//bazel/oci:transitions.bzl", "platform_transition_binary")

# Supported (arch, platform_label, distroless base) tuples. amd64 first
# so the load target tags amd64 on `--platform linux/amd64` hosts; docker
# load picks the matching arch automatically from the index.
_ARCHES = [
    struct(
        arch = "amd64",
        platform = "//platforms:linux_amd64",
        base = "@distroless_cc_linux_amd64",
    ),
    struct(
        arch = "arm64",
        platform = "//platforms:linux_arm64",
        base = "@distroless_cc_linux_arm64_v8",
    ),
]

def _per_arch_image(name, arch_cfg, binary, binary_name, exposed_ports):
    """Build a single-arch oci_image and return its label."""
    suffix = arch_cfg.arch
    transitioned = "_{}_bin_{}_transitioned".format(name, suffix)
    files_target = "_{}_files_{}".format(name, suffix)
    layer_target = "_{}_layer_{}".format(name, suffix)
    image_target = "_{}_image_{}".format(name, suffix)

    platform_transition_binary(
        name = transitioned,
        binary = binary,
        platform = arch_cfg.platform,
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
        name = image_target,
        base = arch_cfg.base,
        entrypoint = ["/" + binary_name],
        exposed_ports = exposed_ports or [],
        tars = [":" + layer_target],
    )

    return ":" + image_target

def konfig_oci_image(
        name,
        binary,
        binary_name,
        repository,
        exposed_ports = None):
    """Build/load/push a multi-arch Konfig container image.

    Args:
      name: package-unique prefix; produces `:image`, `:load`, `:push`.
      binary: label of a rust_binary to package.
      binary_name: filename written into / inside the image (also the entrypoint).
      repository: Docker Hub repository (e.g. "kasa288/konfig").
      exposed_ports: optional list like ["50051/tcp", "9090/tcp"].
    """
    per_arch_labels = [
        _per_arch_image(name, arch_cfg, binary, binary_name, exposed_ports)
        for arch_cfg in _ARCHES
    ]

    oci_image_index(
        name = "image",
        images = per_arch_labels,
    )

    # `:load` loads one arch into the local docker daemon — docker without
    # the containerd image store cannot accept an OCI image index, so we
    # tag the arm64 image (the dev host on Apple Silicon) for the default
    # load path. `:load_amd64` is the amd64 equivalent for Linux CI hosts.
    # `:push` uses the full index for both arches in the remote.
    local_image_label = "_{}_image_arm64".format(name)
    amd64_image_label = "_{}_image_amd64".format(name)

    # Stamped tag list: short git SHA + "latest". The literal "0000000" gets
    # substituted with STABLE_GIT_SHA only when `bazel run --stamp` is used;
    # the placeholder keeps non-stamped builds deterministic and cacheable.
    expand_template(
        name = "_remote_tags",
        out = "_remote_tags.txt",
        stamp_substitutions = {"0000000": "{{STABLE_GIT_SHA}}"},
        template = [
            "0000000",
            "latest",
        ],
    )

    oci_load(
        name = "load",
        image = ":" + local_image_label,
        repo_tags = ["{}:latest".format(repository)],
    )

    oci_load(
        name = "load_amd64",
        image = ":" + amd64_image_label,
        repo_tags = ["{}:latest".format(repository)],
    )

    oci_push(
        name = "push",
        image = ":image",
        remote_tags = ":_remote_tags",
        repository = repository,
    )
