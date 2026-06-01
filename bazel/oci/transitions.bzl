"""Platform transition for OCI image binaries.

`platform_transition_binary` rebuilds its `binary` for the given `platform`
in release configuration, without affecting host-config tools (rules_oci's
load.sh runner, bsd_tar, etc.).

Release knobs baked in:
  - compilation_mode=opt   → -Copt-level=3, codegen tuning, deps in release
  - extra_rustc_flag=
      -Cstrip=debuginfo    → drop DWARF debug info but KEEP the symbol
                             table. eBPF profilers and addr2line need
                             function names to symbolicate stacks; full
                             `-Cstrip=symbols` would erase those too.
                             The size cost over `symbols` is a few hundred
                             KB of `.symtab` + `.strtab`, worth it for
                             on-demand profiling of the default image.
      -Cpanic=abort        → drop unwinding tables (rust panics in containers
                             should never be caught anyway)

Native CPU tuning is intentionally NOT applied: image binaries must run on
any matching-arch host, not just the build machine's microarch.
"""

def _release_platform_transition_impl(_settings, attr):
    return {
        "//command_line_option:platforms": str(attr.platform),
        "//command_line_option:compilation_mode": "opt",
        "@rules_rust//rust/settings:extra_rustc_flag": [
            "-Cstrip=debuginfo",
            "-Cpanic=abort",
        ],
    }

_release_platform_transition = transition(
    implementation = _release_platform_transition_impl,
    inputs = [],
    outputs = [
        "//command_line_option:platforms",
        "//command_line_option:compilation_mode",
        "@rules_rust//rust/settings:extra_rustc_flag",
    ],
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
            cfg = _release_platform_transition,
            executable = True,
            mandatory = True,
        ),
        "platform": attr.label(mandatory = True),
    },
    executable = True,
)
