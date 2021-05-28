package org.apache.thrift.protocol

/**
 * Helper class that encapsulates struct metadata.
 *
 */
class TMessage @JvmOverloads constructor(val name: String? = "", val type: Byte = TType.STOP, val seqid: Int = 0) {
    override fun toString(): String {
        return "<TMessage name:'$name' type: $type seqid:$seqid>"
    }

    override fun hashCode(): Int {
        val prime = 31
        var result = 1
        result = prime * result + (name?.hashCode() ?: 0)
        result = prime * result + seqid
        result = prime * result + type
        return result
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null) return false
        if (javaClass != other.javaClass) return false
        val obj = other as TMessage
        if (name == null) {
            if (obj.name != null) return false
        } else if (name != obj.name) return false
        if (seqid != obj.seqid) return false
        return type == obj.type
    }
}
