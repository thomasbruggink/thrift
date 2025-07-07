all:
	mkdir -p ./out
	rm -rf ./out/*
	$(MAKE) linux-x86-64
	$(MAKE) linux-aarch64
	$(MAKE) windows-x86-64
	$(MAKE) apple-x86-64
	$(MAKE) apple-aarch64

host:
	bazel build //compiler/cpp:thrift

linux-x86-64:
	bazel build --platforms=//:linux_x86-64 //compiler/cpp:thrift
	cp -f bazel-bin/compiler/cpp/thrift ./out/thrift.linux-x86-64

linux-aarch64:
	bazel build --platforms=//:linux_aarch64 //compiler/cpp:thrift
	cp -f bazel-bin/compiler/cpp/thrift ./out/thrift.linux-aarch64

windows-x86-64:
	bazel build --platforms=//:windows_x86-64 //compiler/cpp:thrift
	cp -f bazel-bin/compiler/cpp/thrift.exe ./out/thrift.windows-x86-64

apple-x86-64:
	bazel build --platforms=//:apple_x86-64 //compiler/cpp:thrift --action_env=MACOSX_DEPLOYMENT_TARGET=11.0
	chmod +w bazel-bin/compiler/cpp/thrift
	rcodesign sign --code-signature-flags runtime bazel-bin/compiler/cpp/thrift
	chmod -w bazel-bin/compiler/cpp/thrift
	cp -f bazel-bin/compiler/cpp/thrift ./out/thrift.apple-x86-64

apple-aarch64:
	bazel build --platforms=//:apple_aarch64 //compiler/cpp:thrift
	chmod +w bazel-bin/compiler/cpp/thrift
	rcodesign sign --code-signature-flags runtime bazel-bin/compiler/cpp/thrift
	chmod -w bazel-bin/compiler/cpp/thrift
	cp -f bazel-bin/compiler/cpp/thrift ./out/thrift.apple-aarch64

bison:
	bazel build @bison//:bison

flex:
	bazel build @flex//:flex

clean:
	bazel clean
