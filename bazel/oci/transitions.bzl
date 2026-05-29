"""Platform transition for OCI image binaries.

`platform_transition_binary` rebuilds its `binary` for the given `platform`,
without affecting host-config tools (rules_oci's load.sh runner, bsd_tar, etc.).
This lets `bazel run //docker/...:load` produce a linux/arm64 binary inside the
image while keeping the loader script on darwin_arm64 / linux_amd64 / etc.
"""

def _platforms_transition_impl(_settings, attr):
    return {"//command_line_option:platforms": str(attr.platform)}

_platforms_transition = transition(
    implementation = _platforms_transition_impl,
    inputs = [],
    outputs = ["//command_line_option:platforms"],
)

def _platform_transition_binary_impl(ctx):
    out = ctx.actions.declare_file(ctx.label.name)
    ctx.actions.symlink(
        output = out,
        target_file = ctx.executable.binary,
        is_executable = True,
    )
    return [DefaultInfo(files = depset([out]), executable = out)]

platform_transition_binary = rule(
    implementation = _platform_transition_binary_impl,
    attrs = {
        "binary": attr.label(
            cfg = _platforms_transition,
            executable = True,
            mandatory = True,
        ),
        "platform": attr.label(mandatory = True),
    },
    executable = True,
)
