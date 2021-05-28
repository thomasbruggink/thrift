package org.apache.thrift

internal operator fun String.get(byte: Byte): Char {
    return this.get(byte.toInt())
}
