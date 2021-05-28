package org.apache.thrift

internal operator fun Char.plus(c: Char): Char {
    return (this.toByte() + c.toByte()).toChar()
}
