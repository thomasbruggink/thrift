package org.apache.thrift

import kotlin.experimental.and

internal infix fun Byte.shl(other: Byte): Byte {
    return (this.toInt() shl other.toInt()).toByte()
}

internal infix fun Byte.or(other: Int): Byte {
    return (this.toInt() shl other).toByte()
}

internal infix fun Short.shr(other: Int): Int {
    return (this.toInt() shr other)
}

internal infix fun Byte.shl(other: Int): Byte {
    return (this.toInt() shl other).toByte()
}

internal infix fun Byte.and(other: Int): Byte {
    return (this.toInt() and other).toByte()
}

internal infix fun Byte.andToShort(other: Int): Short {
    return (this.toShort() and other.toShort())
}

internal infix fun Byte.andToInt(other: Int): Int {
    return (this.toInt() and other)
}

internal infix fun Byte.andToLong(other: Int): Long {
    return (this.toLong() and other.toLong())
}

internal infix fun Byte.shr(other: Int): Byte {
    return (this.toInt() shr other).toByte()
}

internal infix fun Short.shl(other: Int): Short {
    return (this.toInt() shl other).toShort()
}
