package org.apache.thrift.protocol

import org.apache.thrift.TException
import org.apache.thrift.scheme.StandardScheme
import org.apache.thrift.transport.TTransport
import java.nio.ByteBuffer


/**
 * Protocol interface definition.
 *
 */
abstract class TProtocol protected constructor(
        /**
         * Transport
         */
        protected val trans_: TTransport
) {
    /**
     * Transport accessor
     */
    val transport: TTransport
        get() = trans_

    @Throws(TException::class)
    protected fun checkReadBytesAvailable(map: TMap) {
        val elemSize = (getMinSerializedSize(map.keyType) + getMinSerializedSize(map.valueType)).toLong()
        trans_.checkReadBytesAvailable(map.size * elemSize)
    }

    @Throws(TException::class)
    protected fun checkReadBytesAvailable(list: TList) {
        trans_.checkReadBytesAvailable((list.size * getMinSerializedSize(list.elemType).toLong()))
    }

    @Throws(TException::class)
    protected fun checkReadBytesAvailable(set: TSet) {
        trans_.checkReadBytesAvailable((set.size * getMinSerializedSize(set.elemType).toLong()))
    }

    /**
     * Return
     * @param type  Returns the minimum amount of bytes needed to store the smallest possible instance of TType.
     * @return
     * @throws TException
     */
    @Throws(TException::class)
    abstract fun getMinSerializedSize(type: Byte): Int

    /**
     * Writing methods.
     */
    @Throws(TException::class)
    abstract suspend fun writeMessageBegin(message: TMessage?)
    @Throws(TException::class)
    abstract suspend fun writeMessageEnd()
    @Throws(TException::class)
    abstract suspend fun writeStructBegin(struct: TStruct?)
    @Throws(TException::class)
    abstract suspend fun writeStructEnd()
    @Throws(TException::class)
    abstract suspend fun writeFieldBegin(field: TField?)
    @Throws(TException::class)
    abstract suspend fun writeFieldEnd()
    @Throws(TException::class)
    abstract suspend fun writeFieldStop()
    @Throws(TException::class)
    abstract suspend fun writeMapBegin(map: TMap?)
    @Throws(TException::class)
    abstract suspend fun writeMapEnd()
    @Throws(TException::class)
    abstract suspend fun writeListBegin(list: TList?)
    @Throws(TException::class)
    abstract suspend fun writeListEnd()
    @Throws(TException::class)
    abstract suspend fun writeSetBegin(set: TSet?)
    @Throws(TException::class)
    abstract suspend fun writeSetEnd()
    @Throws(TException::class)
    abstract suspend fun writeBool(b: Boolean)
    @Throws(TException::class)
    abstract suspend fun writeByte(b: Byte)
    @Throws(TException::class)
    abstract suspend fun writeI16(i16: Short)
    @Throws(TException::class)
    abstract suspend fun writeI32(i32: Int)
    @Throws(TException::class)
    abstract suspend fun writeI64(i64: Long)
    @Throws(TException::class)
    abstract suspend fun writeDouble(dub: Double)
    @Throws(TException::class)
    abstract suspend fun writeString(str: String?)
    @Throws(TException::class)
    abstract suspend fun writeBinary(buf: ByteBuffer?)

    /**
     * Reading methods.
     */
    @Throws(TException::class)
    abstract suspend fun readMessageBegin(): TMessage
    @Throws(TException::class)
    abstract suspend fun readMessageEnd()
    @Throws(TException::class)
    abstract suspend fun readStructBegin(): TStruct
    @Throws(TException::class)
    abstract suspend fun readStructEnd()
    @Throws(TException::class)
    abstract suspend fun readFieldBegin(): TField
    @Throws(TException::class)
    abstract suspend fun readFieldEnd()
    @Throws(TException::class)
    abstract suspend fun readMapBegin(): TMap
    @Throws(TException::class)
    abstract suspend fun readMapEnd()
    @Throws(TException::class)
    abstract suspend fun readListBegin(): TList
    @Throws(TException::class)
    abstract suspend fun readListEnd()
    @Throws(TException::class)
    abstract suspend fun readSetBegin(): TSet
    @Throws(TException::class)
    abstract suspend fun readSetEnd()
    @Throws(TException::class)
    abstract suspend fun readBool(): Boolean
    @Throws(TException::class)
    abstract suspend fun readByte(): Byte
    @Throws(TException::class)
    abstract suspend fun readI16(): Short
    @Throws(TException::class)
    abstract suspend fun readI32(): Int
    @Throws(TException::class)
    abstract suspend fun readI64(): Long
    @Throws(TException::class)
    abstract suspend fun readDouble(): Double
    @Throws(TException::class)
    abstract suspend fun readString(): String
    @Throws(TException::class)
    abstract suspend fun readBinary(): ByteBuffer

    /**
     * Reset any internal state back to a blank slate. This method only needs to
     * be implemented for stateful protocols.
     */
    open fun reset() {}

    /**
     * Scheme accessor
     */
    open val scheme: Class<out Any?>
        get() = StandardScheme::class.java
}
