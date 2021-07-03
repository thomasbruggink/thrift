if(Test-Path ./gen-kotlin) {
    Remove-Item -Recurse -Force ./gen-kotlin
}
../../compiler/cpp/Debug/thrift.exe --gen kotlin --gen java --gen netstd ./JavaTypes.thrift
if(Test-Path ./app/src/main/kotlin/com/testing) {
    Remove-Item -Recurse -Force ./app/src/main/kotlin/com/testing/api
}
Move-Item gen-kotlin/com/testing/api app/src/main/kotlin/com/testing/
