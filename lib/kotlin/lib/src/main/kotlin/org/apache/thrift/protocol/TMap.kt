package org.apache.thrift.protocol

/**
 * Helper class that encapsulates map metadata.
 *
 */
class TMap @JvmOverloads constructor(
    val keyType: Byte = TType.STOP,
    val valueType: Byte = TType.STOP,
    val size: Int = 0
)

