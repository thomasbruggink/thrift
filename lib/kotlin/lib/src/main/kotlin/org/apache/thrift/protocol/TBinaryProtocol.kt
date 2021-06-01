/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.thrift.protocol

import org.apache.thrift.TException
import org.apache.thrift.and
import org.apache.thrift.shl
import org.apache.thrift.shr
import org.apache.thrift.transport.TTransport
import org.apache.thrift.transport.TTransportException
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import kotlin.experimental.or

/**
 * Binary protocol implementation for thrift.
 *
 */
class TBinaryProtocol @JvmOverloads constructor(
    trans: TTransport?,
    /**
     * The maximum number of bytes to read from the transport for
     * variable-length fields (such as strings or binary) or [.NO_LENGTH_LIMIT] for
     * unlimited.
     */
    private val stringLengthLimit_: Long,
    /**
     * The maximum number of elements to read from the network for
     * containers (maps, sets, lists), or [.NO_LENGTH_LIMIT] for unlimited.
     */
    private val containerLengthLimit_: Long,
    protected var strictRead_: Boolean = false,
    protected var strictWrite_: Boolean = true
) :
    TProtocol(trans) {
    private val inoutTemp = ByteArray(8)

    /**
     * Factory
     */
    class Factory @JvmOverloads constructor(
        protected var strictRead_: Boolean = false,
        protected var strictWrite_: Boolean = true,
        protected var stringLengthLimit_: Long = NO_LENGTH_LIMIT,
        protected var containerLengthLimit_: Long = NO_LENGTH_LIMIT
    ) : TProtocolFactory {
        constructor(stringLengthLimit: Long, containerLengthLimit: Long) : this(
            false,
            true,
            stringLengthLimit,
            containerLengthLimit
        ) {
        }

        override fun getProtocol(trans: TTransport?): TProtocol {
            return TBinaryProtocol(trans, stringLengthLimit_, containerLengthLimit_, strictRead_, strictWrite_)
        }
    }

    /**
     * Constructor
     */
    @JvmOverloads
    constructor(trans: TTransport?, strictRead: Boolean = false, strictWrite: Boolean = true) : this(
        trans,
        NO_LENGTH_LIMIT,
        NO_LENGTH_LIMIT,
        strictRead,
        strictWrite
    ) {
    }

    @Throws(TException::class)
    override fun writeMessageBegin(message: TMessage?) {
        if (strictWrite_) {
            val version = VERSION_1 or message!!.type.toInt()
            writeI32(version)
            writeString(message.name)
            writeI32(message.seqid)
        } else {
            writeString(message!!.name)
            writeByte(message.type)
            writeI32(message.seqid)
        }
    }

    @Throws(TException::class)
    override fun writeMessageEnd() {
    }

    @Throws(TException::class)
    override fun writeStructBegin(struct: TStruct?) {
    }

    @Throws(TException::class)
    override fun writeStructEnd() {
    }

    @Throws(TException::class)
    override fun writeFieldBegin(field: TField?) {
        writeByte(field!!.type)
        writeI16(field.id)
    }

    @Throws(TException::class)
    override fun writeFieldEnd() {
    }

    @Throws(TException::class)
    override fun writeFieldStop() {
        writeByte(TType.STOP)
    }

    @Throws(TException::class)
    override fun writeMapBegin(map: TMap?) {
        writeByte(map!!.keyType)
        writeByte(map.valueType)
        writeI32(map.size)
    }

    @Throws(TException::class)
    override fun writeMapEnd() {
    }

    @Throws(TException::class)
    override fun writeListBegin(list: TList?) {
        writeByte(list!!.elemType)
        writeI32(list.size)
    }

    @Throws(TException::class)
    override fun writeListEnd() {
    }

    @Throws(TException::class)
    override fun writeSetBegin(set: TSet?) {
        writeByte(set!!.elemType)
        writeI32(set.size)
    }

    @Throws(TException::class)
    override fun writeSetEnd() {
    }

    @Throws(TException::class)
    override fun writeBool(b: Boolean) {
        writeByte(if (b) 1.toByte() else 0.toByte())
    }

    @Throws(TException::class)
    override fun writeByte(b: Byte) {
        inoutTemp[0] = b
        trans_!!.write(inoutTemp, 0, 1)
    }

    @Throws(TException::class)
    override fun writeI16(i16: Short) {
        inoutTemp[0] = (0xff and (i16 shr 8)).toByte()
        inoutTemp[1] = (0xff and i16.toInt()).toByte()
        trans_!!.write(inoutTemp, 0, 2)
    }

    @Throws(TException::class)
    override fun writeI32(i32: Int) {
        inoutTemp[0] = (0xff and (i32 shr 24)).toByte()
        inoutTemp[1] = (0xff and (i32 shr 16)).toByte()
        inoutTemp[2] = (0xff and (i32 shr 8)).toByte()
        inoutTemp[3] = (0xff and i32).toByte()
        trans_!!.write(inoutTemp, 0, 4)
    }

    @Throws(TException::class)
    override fun writeI64(i64: Long) {
        inoutTemp[0] = (0xff and (i64 shr 56).toInt()).toByte()
        inoutTemp[1] = (0xff and (i64 shr 48).toInt()).toByte()
        inoutTemp[2] = (0xff and (i64 shr 40).toInt()).toByte()
        inoutTemp[3] = (0xff and (i64 shr 32).toInt()).toByte()
        inoutTemp[4] = (0xff and (i64 shr 24).toInt()).toByte()
        inoutTemp[5] = (0xff and (i64 shr 16).toInt()).toByte()
        inoutTemp[6] = (0xff and (i64 shr 8).toInt()).toByte()
        inoutTemp[7] = (0xff and i64.toInt()).toByte()
        trans_!!.write(inoutTemp, 0, 8)
    }

    @Throws(TException::class)
    override fun writeDouble(dub: Double) {
        writeI64(java.lang.Double.doubleToLongBits(dub))
    }

    @Throws(TException::class)
    override fun writeString(str: String?) {
        val dat = str!!.toByteArray(StandardCharsets.UTF_8)
        writeI32(dat.size)
        trans_!!.write(dat, 0, dat.size)
    }

    @Throws(TException::class)
    override fun writeBinary(bin: ByteBuffer?) {
        val length = bin!!.limit() - bin.position()
        writeI32(length)
        trans_!!.write(bin.array(), bin.position() + bin.arrayOffset(), length)
    }

    /**
     * Reading methods.
     */
    @Throws(TException::class)
    override fun readMessageBegin(): TMessage {
        val size = readI32()
        return if (size < 0) {
            val version = size and VERSION_MASK
            if (version != VERSION_1) {
                throw TProtocolException(TProtocolException.BAD_VERSION, "Bad version in readMessageBegin")
            }
            TMessage(readString(), (size and 0x000000ff).toByte(), readI32())
        } else {
            if (strictRead_) {
                throw TProtocolException(
                    TProtocolException.BAD_VERSION,
                    "Missing version in readMessageBegin, old client?"
                )
            }
            TMessage(readStringBody(size), readByte(), readI32())
        }
    }

    @Throws(TException::class)
    override fun readMessageEnd() {
    }

    @Throws(TException::class)
    override fun readStructBegin(): TStruct {
        return ANONYMOUS_STRUCT
    }

    @Throws(TException::class)
    override fun readStructEnd() {
    }

    @Throws(TException::class)
    override fun readFieldBegin(): TField {
        val type = readByte()
        val id = if (type == TType.STOP) 0 else readI16()
        return TField("", type, id)
    }

    @Throws(TException::class)
    override fun readFieldEnd() {
    }

    @Throws(TException::class)
    override fun readMapBegin(): TMap {
        val map = TMap(readByte(), readByte(), readI32())
        checkReadBytesAvailable(map)
        checkContainerReadLength(map.size)
        return map
    }

    @Throws(TException::class)
    override fun readMapEnd() {
    }

    @Throws(TException::class)
    override fun readListBegin(): TList {
        val list = TList(readByte(), readI32())
        checkReadBytesAvailable(list)
        checkContainerReadLength(list.size)
        return list
    }

    @Throws(TException::class)
    override fun readListEnd() {
    }

    @Throws(TException::class)
    override fun readSetBegin(): TSet {
        val set = TSet(readByte(), readI32())
        checkReadBytesAvailable(set)
        checkContainerReadLength(set.size)
        return set
    }

    @Throws(TException::class)
    override fun readSetEnd() {
    }

    @Throws(TException::class)
    override fun readBool(): Boolean {
        return readByte().toInt() == 1
    }

    @Throws(TException::class)
    override fun readByte(): Byte {
        if (trans_!!.bytesRemainingInBuffer >= 1) {
            val b = trans_!!.buffer!![trans_!!.bufferPosition]
            trans_!!.consumeBuffer(1)
            return b
        }
        readAll(inoutTemp, 0, 1)
        return inoutTemp[0]
    }

    @Throws(TException::class)
    override fun readI16(): Short {
        var buf: ByteArray? = inoutTemp
        var off = 0
        if (trans_!!.bytesRemainingInBuffer >= 2) {
            buf = trans_!!.buffer
            off = trans_!!.bufferPosition
            trans_!!.consumeBuffer(2)
        } else {
            readAll(inoutTemp, 0, 2)
        }
        return ((buf!![off] and 0xff shl 8 or
                (buf[off + 1] and 0xff)).toShort())
    }

    @Throws(TException::class)
    override fun readI32(): Int {
        var buf: ByteArray? = inoutTemp
        var off = 0
        if (trans_!!.bytesRemainingInBuffer >= 4) {
            buf = trans_!!.buffer
            off = trans_!!.bufferPosition
            trans_!!.consumeBuffer(4)
        } else {
            readAll(inoutTemp, 0, 4)
        }
        return (buf!![off] and 0xff shl 24 or
                (buf[off + 1] and 0xff shl 16) or
                (buf[off + 2] and 0xff shl 8) or
                (buf[off + 3] and 0xff)).toInt()
    }

    @Throws(TException::class)
    override fun readI64(): Long {
        var buf: ByteArray? = inoutTemp
        var off = 0
        if (trans_!!.bytesRemainingInBuffer >= 8) {
            buf = trans_!!.buffer
            off = trans_!!.bufferPosition
            trans_!!.consumeBuffer(8)
        } else {
            readAll(inoutTemp, 0, 8)
        }
        return (buf!![off] and 0xff).toLong() shl 56 or
                ((buf[off + 1] and 0xff).toLong() shl 48) or
                ((buf[off + 2] and 0xff).toLong() shl 40) or
                ((buf[off + 3] and 0xff).toLong() shl 32) or
                ((buf[off + 4] and 0xff).toLong() shl 24) or
                ((buf[off + 5] and 0xff).toLong() shl 16) or
                ((buf[off + 6] and 0xff).toLong() shl 8) or
                (buf[off + 7] and 0xff).toLong()
    }

    @Throws(TException::class)
    override fun readDouble(): Double {
        return java.lang.Double.longBitsToDouble(readI64())
    }

    @Throws(TException::class)
    override fun readString(): String {
        val size = readI32()
        if (trans_!!.bytesRemainingInBuffer >= size) {
            val s = String(
                trans_!!.buffer!!, trans_!!.bufferPosition,
                size, StandardCharsets.UTF_8
            )
            trans_!!.consumeBuffer(size)
            return s
        }
        return readStringBody(size)
    }

    @Throws(TException::class)
    fun readStringBody(size: Int): String {
        checkStringReadLength(size)
        val buf = ByteArray(size)
        trans_!!.readAll(buf, 0, size)
        return String(buf, StandardCharsets.UTF_8)
    }

    @Throws(TException::class)
    override fun readBinary(): ByteBuffer? {
        val size = readI32()
        checkStringReadLength(size)
        if (trans_!!.bytesRemainingInBuffer >= size) {
            val bb = ByteBuffer.wrap(trans_!!.buffer, trans_!!.bufferPosition, size)
            trans_!!.consumeBuffer(size)
            return bb
        }
        val buf = ByteArray(size)
        trans_!!.readAll(buf, 0, size)
        return ByteBuffer.wrap(buf)
    }

    @Throws(TException::class)
    private fun checkStringReadLength(length: Int) {
        if (length < 0) {
            throw TProtocolException(
                TProtocolException.NEGATIVE_SIZE,
                "Negative length: $length"
            )
        }
        transport!!.checkReadBytesAvailable(length.toLong())
        if (stringLengthLimit_ != NO_LENGTH_LIMIT && length > stringLengthLimit_) {
            throw TProtocolException(
                TProtocolException.SIZE_LIMIT,
                "Length exceeded max allowed: $length"
            )
        }
    }

    @Throws(TProtocolException::class)
    private fun checkContainerReadLength(length: Int) {
        if (length < 0) {
            throw TProtocolException(
                TProtocolException.NEGATIVE_SIZE,
                "Negative length: $length"
            )
        }
        if (containerLengthLimit_ != NO_LENGTH_LIMIT && length > containerLengthLimit_) {
            throw TProtocolException(
                TProtocolException.SIZE_LIMIT,
                "Length exceeded max allowed: $length"
            )
        }
    }

    @Throws(TException::class)
    private fun readAll(buf: ByteArray, off: Int, len: Int): Int {
        return trans_!!.readAll(buf, off, len)
    }

    /**
     *
     * Return the minimum number of bytes a type will consume on the wire
     */
    @Throws(TTransportException::class)
    override fun getMinSerializedSize(type: Byte): Int {
        return when (type.toInt()) {
            0 -> 0 // Stop
            1 -> 0 // Void
            2 -> 1 // Bool sizeof(byte)
            3 -> 1 // Byte sizeof(byte)
            4 -> 8 // Double sizeof(double)
            6 -> 2 // I16 sizeof(short)
            8 -> 4 // I32 sizeof(int)
            10 -> 8 // I64 sizeof(long)
            11 -> 4 // string length sizeof(int)
            12 -> 0 // empty struct
            13 -> 4 // element count Map sizeof(int)
            14 -> 4 // element count Set sizeof(int)
            15 -> 4 // element count List sizeof(int)
            else -> throw TTransportException(TTransportException.UNKNOWN, "unrecognized type code")
        }
    }

    companion object {
        private val ANONYMOUS_STRUCT = TStruct()
        private const val NO_LENGTH_LIMIT: Long = -1
        protected const val VERSION_MASK = -0x10000
        protected const val VERSION_1 = -0x7fff0000
    }
}
