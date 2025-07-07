load("@bazel_tools//tools/build_defs/cc:action_names.bzl", "ACTION_NAMES")
load(
    "@bazel_tools//tools/cpp:cc_toolchain_config_lib.bzl",
    "artifact_name_pattern",
    "feature",
    "flag_group",
    "flag_set",
    "tool_path",
)
load("@rules_cc//cc:defs.bzl", "cc_toolchain")

all_link_actions = [
    ACTION_NAMES.cpp_link_executable,
    ACTION_NAMES.cpp_link_dynamic_library,
    ACTION_NAMES.cpp_link_nodeps_dynamic_library,
]

def _impl(ctx):
    arch = ctx.attr.arch
    gcc_ver = ctx.attr.gcc_ver
    target_triplet = "%s-%s" % (arch, ctx.attr.os_vendor)
    bin_path = "/usr/bin/%s" % (target_triplet)
    tool_paths = [
        tool_path(
            name = "gcc",
            path = "%s-gcc" % (bin_path),
        ),
        tool_path(
            name = "ld",
            path = "%s-ld" % (bin_path),
        ),
        tool_path(
            name = "ar",
            path = "%s-ar" % (bin_path),
        ),
        tool_path(
            name = "cpp",
            path = "%s-cpp" % (bin_path),
        ),
        tool_path(
            name = "gcov",
            path = "%s-gcov" % (bin_path),
        ),
        tool_path(
            name = "nm",
            path = "%s-nm" % (bin_path),
        ),
        tool_path(
            name = "objdump",
            path = "%s-objdump" % (bin_path),
        ),
        tool_path(
            name = "strip",
            path = "%s-strip" % (bin_path),
        ),
    ]

    flags = []
    if ctx.attr.os_vendor == "w64-mingw32":
        flags.append("-l:libstdc++.a")
    else:
        flags.append("-lstdc++")
        
    features = [feature(
            name = "default_linker_flags",
            enabled = True,
            flag_sets = [
                flag_set(
                    actions = all_link_actions,
                    flag_groups = ([
                        flag_group(
                            flags = flags
                        ),
                    ]),
                ),
            ],
        )
    ]

    # Make sure to set the .exe extension for windows cross compilation
    artifact_name_patterns = []
    if ctx.attr.os_vendor == "w64-mingw32":
        artifact_name_patterns.append(
            artifact_name_pattern(
                category_name = "executable",
                prefix = "",
                extension = ".exe",
            ))

    return cc_common.create_cc_toolchain_config_info(
        ctx = ctx,
        features = features,
        cxx_builtin_include_directories = [
            "/usr/lib/gcc-cross/%s/%s/include" % (target_triplet, ctx.attr.gcc_ver),
            "/usr/lib/gcc/%s" % (target_triplet),
            "/usr/share/mingw-w64/include",
            "/usr/%s/include" % (target_triplet),
            "/sdk",
            "/out",
        ],
        toolchain_identifier = "local",
        host_system_name = "local",
        target_system_name = "local",
        target_cpu = arch,
        target_libc = "unknown",
        compiler = "gcc",
        abi_version = "unknown",
        abi_libc_version = "unknown",
        tool_paths = tool_paths,
        artifact_name_patterns = artifact_name_patterns,
    )

cc_toolchain_config = rule(
    implementation = _impl,
    attrs = {
        "arch": attr.string(),
        "gcc_ver": attr.int(),
        "os_vendor": attr.string(),
    },
    provides = [CcToolchainConfigInfo],
)

def gen_toolchain(arch, os, os_vendor, host_arch, gcc_ver):
    cc_toolchain_name = "%s_%s_toolchain_%s" % (os, arch, host_arch)
    cc_toolchain_config_name = "%s_%s_toolchain_config_%s" % (os, arch, host_arch)
    cc_toolchain_config(name = cc_toolchain_config_name, arch = arch, os_vendor = os_vendor, gcc_ver = gcc_ver)

    cc_toolchain(
        name = cc_toolchain_name,
        toolchain_identifier = cc_toolchain_name,
        toolchain_config = ":%s" % (cc_toolchain_config_name),
        all_files = ":empty",
        compiler_files = ":empty",
        dwp_files = ":empty",
        linker_files = ":empty",
        objcopy_files = ":empty",
        strip_files = ":empty",
        supports_param_files = 0,
    )

    native.toolchain(
        name = "cc_toolchain_for_%s_%s_%s" % (os, arch, host_arch),
        toolchain = cc_toolchain_name,
        toolchain_type = "@bazel_tools//tools/cpp:toolchain_type",
        exec_compatible_with = [
            "@platforms//cpu:%s" % (host_arch),
            "@platforms//os:linux",
        ],
        target_compatible_with = [
            "@platforms//cpu:%s" % (arch),
            "@platforms//os:%s" % (os),
        ],
    )
