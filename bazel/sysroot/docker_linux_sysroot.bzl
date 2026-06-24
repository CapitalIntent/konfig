def _quote(value):
    return "'" + value.replace("'", "'\\''") + "'"


def _docker_linux_sysroot_impl(rctx):
    docker = rctx.which("docker")
    if not docker:
        fail("docker_linux_sysroot requires docker on PATH")

    packages = " ".join(rctx.attr.packages)
    out = rctx.path("sysroot")
    script = """
set -euo pipefail
out={out}
image={image}
platform={platform}
packages={packages}
rm -rf "$out"
mkdir -p "$out"
cid=$(docker create --platform "$platform" "$image" sleep infinity)
cleanup() {{ docker rm -f "$cid" >/dev/null 2>&1 || true; }}
trap cleanup EXIT
docker start "$cid" >/dev/null
docker exec "$cid" sh -lc "apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq $packages >/dev/null"
docker export "$cid" | tar -C "$out" -xf -
# Debian package metadata may contain ':' in filenames (for example
# binutils-common:arm64.conffiles), which is not a valid Bazel label segment.
# These files are irrelevant to C/C++ compilation.
find "$out/var/lib/dpkg/info" -name '*:*' -type f -delete 2>/dev/null || true
test -e "$out/usr/include/stdio.h"
test -e "$out/usr/lib/aarch64-linux-gnu/crti.o"
test -e "$out/usr/lib/gcc/aarch64-linux-gnu/13/crtbeginS.o"
""".format(
        out = _quote(str(out)),
        image = _quote(rctx.attr.image),
        platform = _quote(rctx.attr.platform),
        packages = _quote(packages),
    )

    result = rctx.execute(["/bin/sh", "-c", script], timeout = 900)
    if result.return_code != 0:
        fail("failed to create Linux sysroot:\nSTDOUT:\n{}\nSTDERR:\n{}".format(result.stdout, result.stderr))

    rctx.file("BUILD.bazel", "")
    rctx.file("sysroot/BUILD.bazel", """
package(default_visibility = ["//visibility:public"])

filegroup(
    name = "sysroot",
    srcs = glob(["**"]),
)
""")


docker_linux_sysroot = repository_rule(
    implementation = _docker_linux_sysroot_impl,
    attrs = {
        "image": attr.string(default = "ubuntu:24.04"),
        "packages": attr.string_list(default = [
            "build-essential",
            "ca-certificates",
        ]),
        "platform": attr.string(default = "linux/arm64"),
    },
    local = True,
)
