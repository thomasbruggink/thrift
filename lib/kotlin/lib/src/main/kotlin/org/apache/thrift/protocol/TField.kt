package org.apache.thrift.protocol

/**
 * Helper class that encapsulates field metadata.
 *
 * Two fields are considered equal if they have the same type and id.
 */
class TField @JvmOverloads constructor(
    val name: String = "",
    val type: Byte = TType.STOP,
    val id: Short = 0.toShort()
) {
    override fun toString(): String {
        return "<TField name:'$name' type:$type field-id:$id>"
    }

    override fun hashCode(): Int {
        val prime = 31
        var result = 1
        result = prime * result + id
        result = prime * result + type
        return result
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null) return false
        if (javaClass != other.javaClass) return false
        val otherField = other as TField
        return type == otherField.type && id == otherField.id
    }
}
