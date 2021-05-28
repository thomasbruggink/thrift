package org.apache.thrift.protocol

/**
 * Helper class that encapsulates list metadata.
 *
 */
class TList @JvmOverloads constructor(val elemType: Byte = TType.STOP, val size: Int = 0)
