#!/bin/bash
if [ -d ./gen-kotlin ]; then
    rm -Rf ./gen-kotlin
fi
../../compiler/cpp/cmake-build-debug/bin/thrift --gen kotlin --gen java ./JavaTypes.thrift
if [ -d ./app/src/main/kotlin/com/testing ]; then
    rm -Rf ./app/src/main/kotlin/com/testing/api
fi
mv gen-kotlin/com/testing/api app/src/main/kotlin/com/testing/
