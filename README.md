# Apache Thrift

LINE Shop's fork of Apache Thrift.

[![Build Status](https://jenkins.linecorp.com/buildStatus/icon?job=thrift_compiler)](https://jenkins.linecorp.com/job/thrift_compiler/)

Contains the following changes:
- Don't warn on Cocoa namespaces
  - We need this to support iOS which uses an older version of Thrift
- Added Bazel to build the compiler
  - This allows for easier cross environment builds

## Building

Download [bazelisk](https://github.com/bazelbuild/bazelisk/releases), rename it to `bazel` and add the binary to the path.

You can then build the project with
```bash
bazel build //compiler/cpp:thrift
```
The binary will be available in `bazel-bin/compiler/cpp/thrift`. The defalt build includes debug symbols and can be stripped on Linux and Mac like:
```bash
strip bazel-bin/compiler/cpp/thrift
```
Bazel has the tendency to make the binary readonly so if the file can't be stripped make sure to run `chmod 770 bazel-bin/compiler/cpp/thrift` first.

Since our build agents run an older version of GLibc its recommended to build the Linux version on the build agent. There is a [Jenkins job](https://jenkins.linecorp.com/job/thrift_compiler/) available that generates an artifact.

## Uploading a new compiler

The Shop build uses binaries uploaded to [VOS](https://verda.linecorp.com/project/object-storage/thrift/objects/?region=tokyo&service=internal). Each version has the following binaries available:
- `thrift.linux-aarch64`
- `thrift.linux-amd64`
- `thrift.macos-aarch64`
- `thrift.macos-x86_64`
- `thrift.windows-amd64`

They can be downloaded from the following URL:
```
https://line-objects-internal.com/thrift/0.22.0/thrift.linux-amd64
```

---

Older versions are available on [binrepo](http://binrepo.linecorp.com/organizations/LINE-Shop/thrift/).

Make a new directory on with the full version name:
```bash
curl -X MKCOL --user "$CONNECT_USERNAME" http://binrepo.linecorp.com/organizations/LINE-Shop/thrift/0.15.0
```

Then upload the binary with the correct architecture suffix. For example, for Linux use:
```bash
curl -T bazel-bin/compiler/cpp/thrift --user "$CONNECT_USERNAME" http://binrepo.linecorp.com/organizations/LINE-Shop/thrift/0.15.0/thrift.linux-x86_64
```

## Preparing a new Thrift version

When setting up a branch for a new Thrift version, we need to add this README and remove GitHub Actions.

Once Bazel module and build files are set up, we need to manually generate some files since we no longer use Make.
Make sure that GNU Bison >=2.5 is on your PATH.

```bash
cd compiler/cpp
flex -o src/thrift/thriftl.cc src/thrift/thriftl.ll
bison -y -o src/thrift/thrifty.cc --defines=src/thrift/thrifty.h src/thrift/thrifty.yy
cp src/thrift/thrifty.h src/thrift/thrifty.hh
```
