package org.apache.thrift

internal operator fun Char.plus(c: Char): Char {
    return (this.code.toByte() + c.code.toByte()).toChar()
}
