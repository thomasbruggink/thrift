package org.apache.thrift.protocol

import org.apache.thrift.TException
import org.apache.thrift.scheme.StandardScheme
import org.apache.thrift.transport.TTransport
import java.nio.ByteBuffer


/**
 * Protocol interface definition.
 *
 */
abstract class TProtocol {
    /**
     * Prevent direct instantiation
     */
    private constructor() {}

    /**
     * Transport
     */
    protected var trans_: TTransport? = null

    /**
     * Constructor
     */
    protected constructor(trans: TTransport?) {
        trans_ = trans
    }

    /**
     * Transport accessor
     */
    val transport: TTransport?
        get() = trans_

    @Throws(TException::class)
    protected fun checkReadBytesAvailable(map: TMap) {
        val elemSize = (getMinSerializedSize(map.keyType) + getMinSerializedSize(map.valueType)).toLong()
        trans_?.checkReadBytesAvailable(map.size * elemSize)
    }

    @Throws(TException::class)
    protected fun checkReadBytesAvailable(list: TList) {
        trans_?.checkReadBytesAvailable((list.size * getMinSerializedSize(list.elemType).toLong()))
    }

    @Throws(TException::class)
    protected fun checkReadBytesAvailable(set: TSet) {
        trans_?.checkReadBytesAvailable((set.size * getMinSerializedSize(set.elemType).toLong()))
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
    abstract fun writeMessageBegin(message: TMessage?)
    @Throws(TException::class)
    abstract fun writeMessageEnd()
    @Throws(TException::class)
    abstract fun writeStructBegin(struct: TStruct?)
    @Throws(TException::class)
    abstract fun writeStructEnd()
    @Throws(TException::class)
    abstract fun writeFieldBegin(field: TField?)
    @Throws(TException::class)
    abstract fun writeFieldEnd()
    @Throws(TException::class)
    abstract fun writeFieldStop()
    @Throws(TException::class)
    abstract fun writeMapBegin(map: TMap?)
    @Throws(TException::class)
    abstract fun writeMapEnd()
    @Throws(TException::class)
    abstract fun writeListBegin(list: TList?)
    @Throws(TException::class)
    abstract fun writeListEnd()
    @Throws(TException::class)
    abstract fun writeSetBegin(set: TSet?)
    @Throws(TException::class)
    abstract fun writeSetEnd()
    @Throws(TException::class)
    abstract fun writeBool(b: Boolean)
    @Throws(TException::class)
    abstract fun writeByte(b: Byte)
    @Throws(TException::class)
    abstract fun writeI16(i16: Short)
    @Throws(TException::class)
    abstract fun writeI32(i32: Int)
    @Throws(TException::class)
    abstract fun writeI64(i64: Long)
    @Throws(TException::class)
    abstract fun writeDouble(dub: Double)
    @Throws(TException::class)
    abstract fun writeString(str: String?)
    @Throws(TException::class)
    abstract fun writeBinary(buf: ByteBuffer?)

    /**
     * Reading methods.
     */
    @Throws(TException::class)
    abstract fun readMessageBegin(): TMessage
    @Throws(TException::class)
    abstract fun readMessageEnd()
    @Throws(TException::class)
    abstract fun readStructBegin(): TStruct
    @Throws(TException::class)
    abstract fun readStructEnd()
    @Throws(TException::class)
    abstract fun readFieldBegin(): TField
    @Throws(TException::class)
    abstract fun readFieldEnd()
    @Throws(TException::class)
    abstract fun readMapBegin(): TMap
    @Throws(TException::class)
    abstract fun readMapEnd()
    @Throws(TException::class)
    abstract fun readListBegin(): TList
    @Throws(TException::class)
    abstract fun readListEnd()
    @Throws(TException::class)
    abstract fun readSetBegin(): TSet
    @Throws(TException::class)
    abstract fun readSetEnd()
    @Throws(TException::class)
    abstract fun readBool(): Boolean
    @Throws(TException::class)
    abstract fun readByte(): Byte
    @Throws(TException::class)
    abstract fun readI16(): Short
    @Throws(TException::class)
    abstract fun readI32(): Int
    @Throws(TException::class)
    abstract fun readI64(): Long
    @Throws(TException::class)
    abstract fun readDouble(): Double
    @Throws(TException::class)
    abstract fun readString(): String
    @Throws(TException::class)
    abstract fun readBinary(): ByteBuffer?

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
