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
import org.apache.thrift.andToInt
import org.apache.thrift.andToLong
import org.apache.thrift.andToShort
import org.apache.thrift.shl
import org.apache.thrift.shr
import org.apache.thrift.transport.TTransport
import org.apache.thrift.transport.TTransportException
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import kotlin.experimental.or

/**
 * TCompactProtocol is the Kotlin implementation of the compact protocol specified
 * in THRIFT-110. The fundamental approach to reducing the overhead of
 * structures is a) use variable-length integers all over the place and b) make
 * use of unused bits wherever possible. Your savings will obviously vary
 * based on the specific makeup of your structs, but in general, the more
 * fields, nested structures, short strings and collections, and low-value i32
 * and i64 fields you have, the more benefit you'll see.
 */
open class TCompactProtocol @JvmOverloads constructor(
    transport: TTransport,
    /**
     * The maximum number of bytes to read from the transport for
     * variable-length fields (such as strings or binary) or [.NO_LENGTH_LIMIT] for
     * unlimited.
     */
    private val stringLengthLimit_: Long = NO_LENGTH_LIMIT,
    /**
     * The maximum number of elements to read from the network for
     * containers (maps, sets, lists), or [.NO_LENGTH_LIMIT] for unlimited.
     */
    private val containerLengthLimit_: Long = NO_LENGTH_LIMIT
) :
    TProtocol(transport) {
    companion object {
        private val EMPTY_BYTES = ByteArray(0)
        private val EMPTY_BUFFER = ByteBuffer.wrap(EMPTY_BYTES)
        private const val NO_LENGTH_LIMIT: Long = -1
        private val ANONYMOUS_STRUCT = TStruct("")
        private val TSTOP = TField("", TType.STOP, 0.toShort())
        private val ttypeToCompactType = ByteArray(16)
        private const val PROTOCOL_ID = 0x82.toByte()
        private const val VERSION: Byte = 1
        private const val VERSION_MASK: Byte = 0x1f // 0001 1111
        private const val TYPE_MASK = 0xE0.toByte() // 1110 0000
        private const val TYPE_BITS: Byte = 0x07 // 0000 0111
        private const val TYPE_SHIFT_AMOUNT = 5

        init {
            ttypeToCompactType[TType.STOP.toInt()] = TType.STOP
            ttypeToCompactType[TType.BOOL.toInt()] = Types.BOOLEAN_TRUE
            ttypeToCompactType[TType.BYTE.toInt()] = Types.BYTE
            ttypeToCompactType[TType.I16.toInt()] = Types.I16
            ttypeToCompactType[TType.I32.toInt()] = Types.I32
            ttypeToCompactType[TType.I64.toInt()] = Types.I64
            ttypeToCompactType[TType.DOUBLE.toInt()] = Types.DOUBLE
            ttypeToCompactType[TType.STRING.toInt()] = Types.BINARY
            ttypeToCompactType[TType.LIST.toInt()] = Types.LIST
            ttypeToCompactType[TType.SET.toInt()] = Types.SET
            ttypeToCompactType[TType.MAP.toInt()] = Types.MAP
            ttypeToCompactType[TType.STRUCT.toInt()] = Types.STRUCT
        }
    }

    /**
     * TProtocolFactory that produces TCompactProtocols.
     */
    class Factory constructor(
        private val stringLengthLimit_: Long = NO_LENGTH_LIMIT,
        private val containerLengthLimit_: Long = NO_LENGTH_LIMIT
    ) : TProtocolFactory {
        override fun getProtocol(trans: TTransport): TProtocol {
            return TCompactProtocol(trans, stringLengthLimit_, containerLengthLimit_)
        }
    }

    /**
     * All of the on-wire type codes.
     */
    private object Types {
        const val BOOLEAN_TRUE: Byte = 0x01
        const val BOOLEAN_FALSE: Byte = 0x02
        const val BYTE: Byte = 0x03
        const val I16: Byte = 0x04
        const val I32: Byte = 0x05
        const val I64: Byte = 0x06
        const val DOUBLE: Byte = 0x07
        const val BINARY: Byte = 0x08
        const val LIST: Byte = 0x09
        const val SET: Byte = 0x0A
        const val MAP: Byte = 0x0B
        const val STRUCT: Byte = 0x0C
    }

    /**
     * Used to keep track of the last field for the current and previous structs,
     * so we can do the delta stuff.
     */
    private val lastField_ = ShortStack(15)
    private var lastFieldId_: Short = 0

    /**
     * If we encounter a boolean field begin, save the TField here so it can
     * have the value incorporated.
     */
    private var booleanField_: TField? = null

    /**
     * If we read a field header, and it's a boolean field, save the boolean
     * value here so that readBool can use it.
     */
    private var boolValue_: Boolean? = null

    /**
     * Temporary buffer used for various operations that would otherwise require a
     * small allocation.
     */
    private val temp = ByteArray(10)

    override fun reset() {
        lastField_.clear()
        lastFieldId_ = 0
    }
    //
    // Public Writing methods.
    //
    /**
     * Write a message header to the wire. Compact Protocol messages contain the
     * protocol version so we can migrate forwards in the future if need be.
     */
    @Throws(TException::class)
    override suspend fun writeMessageBegin(message: TMessage?) {
        writeByteDirect(PROTOCOL_ID)
        writeByteDirect(VERSION and VERSION_MASK.toInt() or (message!!.type shl TYPE_SHIFT_AMOUNT and TYPE_MASK.toInt()))
        writeVarint32(message.seqid)
        writeString(message.name)
    }

    /**
     * Write a struct begin. This doesn't actually put anything on the wire. We
     * use it as an opportunity to put special placeholder markers on the field
     * stack so we can get the field id deltas correct.
     */
    @Throws(TException::class)
    override suspend fun writeStructBegin(struct: TStruct?) {
        lastField_.push(lastFieldId_)
        lastFieldId_ = 0
    }

    /**
     * Write a struct end. This doesn't actually put anything on the wire. We use
     * this as an opportunity to pop the last field from the current struct off
     * of the field stack.
     */
    @Throws(TException::class)
    override suspend fun writeStructEnd() {
        lastFieldId_ = lastField_.pop()
    }

    /**
     * Write a field header containing the field id and field type. If the
     * difference between the current field id and the last one is small (&lt; 15),
     * then the field id will be encoded in the 4 MSB as a delta. Otherwise, the
     * field id will follow the type header as a zigzag varint.
     */
    @Throws(TException::class)
    override suspend fun writeFieldBegin(field: TField?) {
        if (field!!.type == TType.BOOL) {
            // we want to possibly include the value, so we'll wait.
            booleanField_ = field
        } else {
            writeFieldBeginInternal(field, (-1).toByte())
        }
    }

    /**
     * The workhorse of writeFieldBegin. It has the option of doing a
     * 'type override' of the type header. This is used specifically in the
     * boolean field case.
     */
    @Throws(TException::class)
    private suspend fun writeFieldBeginInternal(field: TField?, typeOverride: Byte) {
        // short lastField = lastField_.pop();

        // if there's a type override, use that.
        val typeToWrite = if (typeOverride.toInt() == -1) getCompactType(field!!.type) else typeOverride

        // check if we can use delta encoding for the field id
        if (field!!.id > lastFieldId_ && field.id - lastFieldId_ <= 15) {
            // write them together
            writeByteDirect(field.id - lastFieldId_ shl 4 or typeToWrite.toInt())
        } else {
            // write them separate
            writeByteDirect(typeToWrite)
            writeI16(field.id)
        }
        lastFieldId_ = field.id
        // lastField_.push(field.id);
    }

    /**
     * Write the STOP symbol so we know there are no more fields in this struct.
     */
    @Throws(TException::class)
    override suspend fun writeFieldStop() {
        writeByteDirect(TType.STOP)
    }

    /**
     * Write a map header. If the map is empty, omit the key and value type
     * headers, as we don't need any additional information to skip it.
     */
    @Throws(TException::class)
    override suspend fun writeMapBegin(map: TMap?) {
        if (map!!.size == 0) {
            writeByteDirect(0)
        } else {
            writeVarint32(map.size)
            writeByteDirect(getCompactType(map.keyType) shl 4 or getCompactType(map.valueType))
        }
    }

    /**
     * Write a list header.
     */
    @Throws(TException::class)
    override suspend fun writeListBegin(list: TList?) {
        writeCollectionBegin(list!!.elemType, list.size)
    }

    /**
     * Write a set header.
     */
    @Throws(TException::class)
    override suspend fun writeSetBegin(set: TSet?) {
        writeCollectionBegin(set!!.elemType, set.size)
    }

    /**
     * Write a boolean value. Potentially, this could be a boolean field, in
     * which case the field header info isn't written yet. If so, decide what the
     * right type header is for the value and then write the field header.
     * Otherwise, write a single byte.
     */
    @Throws(TException::class)
    override suspend fun writeBool(b: Boolean) {
        if (booleanField_ != null) {
            // we haven't written the field header yet
            writeFieldBeginInternal(booleanField_, if (b) Types.BOOLEAN_TRUE else Types.BOOLEAN_FALSE)
            booleanField_ = null
        } else {
            // we're not part of a field, so just write the value.
            writeByteDirect(if (b) Types.BOOLEAN_TRUE else Types.BOOLEAN_FALSE)
        }
    }

    /**
     * Write a byte. Nothing to see here!
     */
    @Throws(TException::class)
    override suspend fun writeByte(b: Byte) {
        writeByteDirect(b)
    }

    /**
     * Write an I16 as a zigzag varint.
     */
    @Throws(TException::class)
    override suspend fun writeI16(i16: Short) {
        writeVarint32(intToZigZag(i16.toInt()))
    }

    /**
     * Write an i32 as a zigzag varint.
     */
    @Throws(TException::class)
    override suspend fun writeI32(i32: Int) {
        writeVarint32(intToZigZag(i32))
    }

    /**
     * Write an i64 as a zigzag varint.
     */
    @Throws(TException::class)
    override suspend fun writeI64(i64: Long) {
        writeVarint64(longToZigzag(i64))
    }

    /**
     * Write a double to the wire as 8 bytes.
     */
    @Throws(TException::class)
    override suspend fun writeDouble(dub: Double) {
        fixedLongToBytes(java.lang.Double.doubleToLongBits(dub), temp, 0)
        trans_.write(temp, 0, 8)
    }

    /**
     * Write a string to the wire with a varint size preceding.
     */
    @Throws(TException::class)
    override suspend fun writeString(str: String?) {
        val bytes = str!!.toByteArray(StandardCharsets.UTF_8)
        writeVarint32(bytes.size)
        trans_.write(bytes, 0, bytes.size)
    }

    /**
     * Write a byte array, using a varint for the size.
     */
    @Throws(TException::class)
    override suspend fun writeBinary(buf: ByteBuffer?) {
        val bb = buf!!.asReadOnlyBuffer()
        writeVarint32(bb.remaining())
        trans_.write(bb)
    }

    //
    // These methods are called by structs, but don't actually have any wire
    // output or purpose.
    //
    @Throws(TException::class)
    override suspend fun writeMessageEnd() {
    }

    @Throws(TException::class)
    override suspend fun writeMapEnd() {
    }

    @Throws(TException::class)
    override suspend fun writeListEnd() {
    }

    @Throws(TException::class)
    override suspend fun writeSetEnd() {
    }

    @Throws(TException::class)
    override suspend fun writeFieldEnd() {
    }
    //
    // Internal writing methods
    //
    /**
     * Abstract method for writing the start of lists and sets. List and sets on
     * the wire differ only by the type indicator.
     */
    @Throws(TException::class)
    protected suspend fun writeCollectionBegin(elemType: Byte, size: Int) {
        if (size <= 14) {
            writeByteDirect(size shl 4 or getCompactType(elemType).toInt())
        } else {
            writeByteDirect(0xf0 or getCompactType(elemType).toInt())
            writeVarint32(size)
        }
    }

    /**
     * Write an i32 as a varint. Results in 1-5 bytes on the wire.
     * TODO: make a permanent buffer like writeVarint64?
     */
    @Throws(TException::class)
    private suspend fun writeVarint32(n: Int) {
        var localN = n
        var idx = 0
        while (true) {
            if (n and 0x7F.inv() == 0) {
                temp[idx++] = localN.toByte()
                // writeByteDirect((byte)n);
                break
                // return;
            } else {
                temp[idx++] = (localN and 0x7F or 0x80).toByte()
                // writeByteDirect((byte)((n & 0x7F) | 0x80));
                localN = localN ushr 7
            }
        }
        trans_.write(temp, 0, idx)
    }

    /**
     * Write an i64 as a varint. Results in 1-10 bytes on the wire.
     */
    @Throws(TException::class)
    private suspend fun writeVarint64(n: Long) {
        var localN = n
        var idx = 0
        while (true) {
            if (localN and 0x7FL.inv() == 0L) {
                temp[idx++] = localN.toByte()
                break
            } else {
                temp[idx++] = (localN and 0x7F or 0x80).toByte()
                localN = n ushr 7
            }
        }
        trans_.write(temp, 0, idx)
    }

    /**
     * Convert l into a zigzag long. This allows negative numbers to be
     * represented compactly as a varint.
     */
    private fun longToZigzag(l: Long): Long {
        return l shl 1 xor (l shr 63)
    }

    /**
     * Convert n into a zigzag int. This allows negative numbers to be
     * represented compactly as a varint.
     */
    private fun intToZigZag(n: Int): Int {
        return n shl 1 xor (n shr 31)
    }

    /**
     * Convert a long into little-endian bytes in buf starting at off and going
     * until off+7.
     */
    private fun fixedLongToBytes(n: Long, buf: ByteArray, off: Int) {
        buf[off + 0] = (n and 0xff).toByte()
        buf[off + 1] = (n shr 8 and 0xff).toByte()
        buf[off + 2] = (n shr 16 and 0xff).toByte()
        buf[off + 3] = (n shr 24 and 0xff).toByte()
        buf[off + 4] = (n shr 32 and 0xff).toByte()
        buf[off + 5] = (n shr 40 and 0xff).toByte()
        buf[off + 6] = (n shr 48 and 0xff).toByte()
        buf[off + 7] = (n shr 56 and 0xff).toByte()
    }

    /**
     * Writes a byte without any possibility of all that field header nonsense.
     * Used internally by other writing methods that know they need to write a byte.
     */
    @Throws(TException::class)
    private suspend fun writeByteDirect(b: Byte) {
        temp[0] = b
        trans_.write(temp, 0, 1)
    }

    /**
     * Writes a byte without any possibility of all that field header nonsense.
     */
    @Throws(TException::class)
    private suspend fun writeByteDirect(n: Int) {
        writeByteDirect(n.toByte())
    }
    //
    // Reading methods.
    //
    /**
     * Read a message header.
     */
    @Throws(TException::class)
    override suspend fun readMessageBegin(): TMessage {
        val protocolId = readByte()
        if (protocolId != PROTOCOL_ID) {
            throw TProtocolException(
                "Expected protocol id ${Integer.toHexString(PROTOCOL_ID.toInt())} but got ${
                    Integer.toHexString(
                        protocolId.toInt()
                    )
                }"
            )
        }
        val versionAndType = readByte()
        val version = (versionAndType and VERSION_MASK.toInt())
        if (version != VERSION) {
            throw TProtocolException("Expected version $VERSION but got $version")
        }
        val type = (versionAndType shr TYPE_SHIFT_AMOUNT and TYPE_BITS.toInt())
        val seqid = readVarint32()
        val messageName = readString()
        return TMessage(messageName, type, seqid)
    }

    /**
     * Read a struct begin. There's nothing on the wire for this, but it is our
     * opportunity to push a new struct begin marker onto the field stack.
     */
    @Throws(TException::class)
    override suspend fun readStructBegin(): TStruct {
        lastField_.push(lastFieldId_)
        lastFieldId_ = 0
        return ANONYMOUS_STRUCT
    }

    /**
     * Doesn't actually consume any wire data, just removes the last field for
     * this struct from the field stack.
     */
    @Throws(TException::class)
    override suspend fun readStructEnd() {
        // consume the last field we read off the wire.
        lastFieldId_ = lastField_.pop()
    }

    /**
     * Read a field header off the wire.
     */
    @Throws(TException::class)
    override suspend fun readFieldBegin(): TField {
        val type = readByte()

        // if it's a stop, then we can return immediately, as the struct is over.
        if (type == TType.STOP) {
            return TSTOP
        }
        val fieldId: Short

        // mask off the 4 MSB of the type header. it could contain a field id delta.
        val modifier = type andToShort 0xf0 shr 4
        fieldId = if (modifier == 0) {
            // not a delta. look ahead for the zigzag varint field id.
            readI16()
        } else {
            // has a delta. add the delta to the last read field id.
            (lastFieldId_ + modifier).toShort()
        }
        val field = TField("", getTType((type and 0x0f)), fieldId)

        // if this happens to be a boolean field, the value is encoded in the type
        if (isBoolType(type)) {
            // save the boolean value in a special instance variable.
            boolValue_ =
                if ((type and 0x0f) == Types.BOOLEAN_TRUE) java.lang.Boolean.TRUE else java.lang.Boolean.FALSE
        }

        // push the new field onto the field stack so we can keep the deltas going.
        lastFieldId_ = field.id
        return field
    }

    /**
     * Read a map header off the wire. If the size is zero, skip reading the key
     * and value type. This means that 0-length maps will yield TMaps without the
     * "correct" types.
     */
    @Throws(TException::class)
    override suspend fun readMapBegin(): TMap {
        val size = readVarint32()
        checkContainerReadLength(size)
        val keyAndValueType = if (size == 0) 0 else readByte()
        val map = TMap(getTType((keyAndValueType shr 4)), getTType((keyAndValueType and 0xf)), size)
        checkReadBytesAvailable(map)
        return map
    }

    /**
     * Read a list header off the wire. If the list size is 0-14, the size will
     * be packed into the element type header. If it's a longer list, the 4 MSB
     * of the element type header will be 0xF, and a varint will follow with the
     * true size.
     */
    @Throws(TException::class)
    override suspend fun readListBegin(): TList {
        val sizeAndType = readByte()
        var size: Int = (sizeAndType shr 4 and 0x0f).toInt()
        if (size == 15) {
            size = readVarint32()
        }
        checkContainerReadLength(size)
        val list = TList(getTType(sizeAndType), size)
        checkReadBytesAvailable(list)
        return list
    }

    /**
     * Read a set header off the wire. If the set size is 0-14, the size will
     * be packed into the element type header. If it's a longer set, the 4 MSB
     * of the element type header will be 0xF, and a varint will follow with the
     * true size.
     */
    @Throws(TException::class)
    override suspend fun readSetBegin(): TSet {
        return TSet(readListBegin())
    }

    /**
     * Read a boolean off the wire. If this is a boolean field, the value should
     * already have been read during readFieldBegin, so we'll just consume the
     * pre-stored value. Otherwise, read a byte.
     */
    @Throws(TException::class)
    override suspend fun readBool(): Boolean {
        if (boolValue_ != null) {
            val result: Boolean = boolValue_ as Boolean
            boolValue_ = null
            return result
        }
        return readByte() == Types.BOOLEAN_TRUE
    }

    /**
     * Read a single byte off the wire. Nothing interesting here.
     */
    @Throws(TException::class)
    override suspend fun readByte(): Byte {
        val b: Byte
        if (trans_.bytesRemainingInBuffer > 0) {
            b = trans_.buffer!![trans_.bufferPosition]
            trans_.consumeBuffer(1)
        } else {
            trans_.readAll(temp, 0, 1)
            b = temp[0]
        }
        return b
    }

    /**
     * Read an i16 from the wire as a zigzag varint.
     */
    @Throws(TException::class)
    override suspend fun readI16(): Short {
        return zigzagToInt(readVarint32()).toShort()
    }

    /**
     * Read an i32 from the wire as a zigzag varint.
     */
    @Throws(TException::class)
    override suspend fun readI32(): Int {
        return zigzagToInt(readVarint32())
    }

    /**
     * Read an i64 from the wire as a zigzag varint.
     */
    @Throws(TException::class)
    override suspend fun readI64(): Long {
        return zigzagToLong(readVarint64())
    }

    /**
     * No magic here - just read a double off the wire.
     */
    @Throws(TException::class)
    override suspend fun readDouble(): Double {
        trans_.readAll(temp, 0, 8)
        return java.lang.Double.longBitsToDouble(bytesToLong(temp))
    }

    /**
     * Reads a byte[] (via readBinary), and then UTF-8 decodes it.
     */
    @Throws(TException::class)
    override suspend fun readString(): String {
        val length = readVarint32()
        checkStringReadLength(length)
        if (length == 0) {
            return ""
        }
        val str: String
        if (trans_.bytesRemainingInBuffer >= length) {
            str = String(
                trans_.buffer!!, trans_.bufferPosition,
                length, StandardCharsets.UTF_8
            )
            trans_.consumeBuffer(length)
        } else {
            str = String(readBinary(length), StandardCharsets.UTF_8)
        }
        return str
    }

    /**
     * Read a ByteBuffer from the wire.
     */
    @Throws(TException::class)
    override suspend fun readBinary(): ByteBuffer {
        val length = readVarint32()
        if (length == 0) {
            return EMPTY_BUFFER
        }
        transport.checkReadBytesAvailable(length.toLong())
        if (trans_.bytesRemainingInBuffer >= length) {
            val bb = ByteBuffer.wrap(trans_.buffer, trans_.bufferPosition, length)
            trans_.consumeBuffer(length)
            return bb
        }
        val buf = ByteArray(length)
        trans_.readAll(buf, 0, length)
        return ByteBuffer.wrap(buf)
    }

    /**
     * Read a byte[] of a known length from the wire.
     */
    @Throws(TException::class)
    private suspend fun readBinary(length: Int): ByteArray {
        if (length == 0) return EMPTY_BYTES
        val buf = ByteArray(length)
        trans_.readAll(buf, 0, length)
        return buf
    }

    @Throws(TException::class)
    private fun checkStringReadLength(length: Int) {
        if (length < 0) {
            throw TProtocolException(
                TProtocolException.NEGATIVE_SIZE,
                "Negative length: $length"
            )
        }
        transport.checkReadBytesAvailable(length.toLong())
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

    //
    // These methods are here for the struct to call, but don't have any wire
    // encoding.
    //
    @Throws(TException::class)
    override suspend fun readMessageEnd() {
    }

    @Throws(TException::class)
    override suspend fun readFieldEnd() {
    }

    @Throws(TException::class)
    override suspend fun readMapEnd() {
    }

    @Throws(TException::class)
    override suspend fun readListEnd() {
    }

    @Throws(TException::class)
    override suspend fun readSetEnd() {
    }
    //
    // Internal reading methods
    //
    /**
     * Read an i32 from the wire as a varint. The MSB of each byte is set
     * if there is another byte to follow. This can read up to 5 bytes.
     */
    @Throws(TException::class)
    private suspend fun readVarint32(): Int {
        var result = 0
        var shift = 0
        if (trans_.bytesRemainingInBuffer >= 5) {
            val buf = trans_.buffer
            val pos = trans_.bufferPosition
            var off = 0
            while (true) {
                val b = buf!![pos + off]
                result = result or ((b andToInt 0x7f) shl shift)
                if ((b and 0x80).toInt() != 0x80) break
                shift += 7
                off++
            }
            trans_.consumeBuffer(off + 1)
        } else {
            while (true) {
                val b = readByte()
                result = result or ((b andToInt 0x7f) shl shift)
                if ((b and 0x80).toInt() != 0x80) break
                shift += 7
            }
        }
        return result
    }

    /**
     * Read an i64 from the wire as a proper varint. The MSB of each byte is set
     * if there is another byte to follow. This can read up to 10 bytes.
     */
    @Throws(TException::class)
    private suspend fun readVarint64(): Long {
        var shift = 0
        var result: Long = 0
        if (trans_.bytesRemainingInBuffer >= 10) {
            val buf = trans_.buffer
            val pos = trans_.bufferPosition
            var off = 0
            while (true) {
                val b = buf!![pos + off]
                result = result or ((b andToLong 0x7f) shl shift)
                if ((b and 0x80).toInt() != 0x80) break
                shift += 7
                off++
            }
            trans_.consumeBuffer(off + 1)
        } else {
            while (true) {
                val b = readByte()
                result = result or ((b andToLong 0x7f) shl shift)
                if ((b and 0x80).toInt() != 0x80) break
                shift += 7
            }
        }
        return result
    }
    //
    // encoding helpers
    //
    /**
     * Convert from zigzag int to int.
     */
    private fun zigzagToInt(n: Int): Int {
        return n ushr 1 xor -(n and 1)
    }

    /**
     * Convert from zigzag long to long.
     */
    private fun zigzagToLong(n: Long): Long {
        return n ushr 1 xor -(n and 1)
    }

    /**
     * Note that it's important that the mask bytes are long literals,
     * otherwise they'll default to ints, and when you shift an int left 56 bits,
     * you just get a messed up int.
     */
    private fun bytesToLong(bytes: ByteArray): Long {
        return (bytes[7] and 0xff shl 56 or
                (bytes[6] and 0xff shl 48) or
                (bytes[5] and 0xff shl 40) or
                (bytes[4] and 0xff shl 32) or
                (bytes[3] and 0xff shl 24) or
                (bytes[2] and 0xff shl 16) or
                (bytes[1] and 0xff shl 8) or
                (bytes[0] and 0xff)).toLong()
    }

    //
    // type testing and converting
    //
    private fun isBoolType(b: Byte): Boolean {
        val lowerNibble: Int = (b and 0x0f).toInt()
        return lowerNibble == Types.BOOLEAN_TRUE.toInt() || lowerNibble == Types.BOOLEAN_FALSE.toInt()
    }

    /**
     * Given a TCompactProtocol.Types constant, convert it to its corresponding
     * TType value.
     */
    @Throws(TProtocolException::class)
    private fun getTType(type: Byte): Byte {
        return when ((type and 0x0f)) {
            TType.STOP -> TType.STOP
            Types.BOOLEAN_FALSE, Types.BOOLEAN_TRUE -> TType.BOOL
            Types.BYTE -> TType.BYTE
            Types.I16 -> TType.I16
            Types.I32 -> TType.I32
            Types.I64 -> TType.I64
            Types.DOUBLE -> TType.DOUBLE
            Types.BINARY -> TType.STRING
            Types.LIST -> TType.LIST
            Types.SET -> TType.SET
            Types.MAP -> TType.MAP
            Types.STRUCT -> TType.STRUCT
            else -> throw TProtocolException("don't know what type: " + (type and 0x0f) as Byte)
        }
    }

    /**
     * Given a TType value, find the appropriate TCompactProtocol.Types constant.
     */
    private fun getCompactType(ttype: Byte): Byte {
        return ttypeToCompactType[ttype.toInt()]
    }

    /**
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
            6 -> 1 // I16 sizeof(byte)
            8 -> 1 // I32 sizeof(byte)
            10 -> 1 // I64 sizeof(byte)
            11 -> 1 // string length sizeof(byte)
            12 -> 0 // empty struct
            13 -> 1 // element count Map sizeof(byte)
            14 -> 1 // element count Set sizeof(byte)
            15 -> 1 // element count List sizeof(byte)
            else -> throw TTransportException(TTransportException.UNKNOWN, "unrecognized type code")
        }
    }
}
