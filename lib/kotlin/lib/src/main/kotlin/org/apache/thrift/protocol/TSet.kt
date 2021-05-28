package org.apache.thrift.protocol

/**
 * Helper class that encapsulates set metadata.
 *
 */
class TSet @JvmOverloads constructor(val elemType: Byte = TType.STOP, val size: Int = 0) {
    constructor(list: TList) : this(list.elemType, list.size) {}
}
