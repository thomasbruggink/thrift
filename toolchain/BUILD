load(":thrift_toolchain.bzl", "gen_toolchain")

package(default_visibility = ["//visibility:public"])

filegroup(name = "empty")

# From x86_64
gen_toolchain(arch = "aarch64", os = "linux", vendor = "gnu", host_arch = "x86_64", gcc_ver = 11, constraints = [])
gen_toolchain(arch = "x86_64", os = "w64", vendor = "mingw32", host_arch = "x86_64", gcc_ver = 11, constraints = [ "//:windows-cross" ])
# From aarch64
gen_toolchain(arch = "x86_64", os = "linux", vendor = "gnu", host_arch = "aarch64", gcc_ver = 11, constraints = [])
