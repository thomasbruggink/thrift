load(":thrift_toolchain.bzl", "gen_toolchain")

package(default_visibility = ["//visibility:public"])

filegroup(name = "empty")

gcc_ver=13

# From x86_64
gen_toolchain(arch = "aarch64", os = "linux", os_vendor = "linux-gnu", host_arch = "x86_64", gcc_ver = gcc_ver)
gen_toolchain(arch = "aarch64", os = "osx", os_vendor = "apple-darwin24", host_arch = "x86_64", gcc_ver = 14)
gen_toolchain(arch = "x86_64", os = "osx", os_vendor = "apple-darwin24", host_arch = "x86_64", gcc_ver = 14)
gen_toolchain(arch = "x86_64", os = "windows", os_vendor = "w64-mingw32", host_arch = "x86_64", gcc_ver = gcc_ver)
gen_toolchain(arch = "aarch64", os = "windows", os_vendor = "w64-mingw32", host_arch = "x86_64", gcc_ver = 15)
# From aarch64
gen_toolchain(arch = "x86_64", os = "linux", os_vendor = "linux-gnu", host_arch = "aarch64", gcc_ver = gcc_ver)
gen_toolchain(arch = "x86_64", os = "windows", os_vendor = "w64-mingw32", host_arch = "aarch64", gcc_ver = gcc_ver)
gen_toolchain(arch = "aarch64", os = "osx", os_vendor = "apple-darwin24", host_arch = "aarch64", gcc_ver = 14)
gen_toolchain(arch = "x86_64", os = "osx", os_vendor = "apple-darwin24", host_arch = "aarch64", gcc_ver = 14)
gen_toolchain(arch = "aarch64", os = "windows", os_vendor = "w64-mingw32", host_arch = "aarch64", gcc_ver = 15)
