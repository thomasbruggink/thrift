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

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.apache.thrift.TByteArrayOutputStream
import org.apache.thrift.TException
import org.apache.thrift.and
import org.apache.thrift.plus
import org.apache.thrift.shl
import org.apache.thrift.shr
import org.apache.thrift.transport.TTransport
import org.apache.thrift.transport.TTransportException
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.Stack

/**
 * JSON protocol implementation for thrift.
 *
 * This is a full-featured protocol supporting write and read.
 *
 * Please see the C++ class header for a detailed description of the
 * protocol's wire format.
 *
 */
class TJSONProtocol(
        trans: TTransport
) : TProtocol(trans) {
    /**
     * Factory for JSON protocol objects
     */
    class Factory(
            private val fieldNamesAsString: Boolean = false
    ) : TProtocolFactory {
        override fun getProtocol(trans: TTransport): TProtocol {
            return TJSONProtocol(trans, fieldNamesAsString)
        }
    }

    // Base class for tracking JSON contexts that may require inserting/reading
    // additional JSON syntax characters
    // This base context does nothing.
    protected open inner class JSONBaseContext {
        @Throws(TException::class)
        open suspend fun write() {
        }

        @Throws(TException::class)
        open suspend fun read() {
        }

        open fun escapeNum(): Boolean {
            return false
        }
    }

    // Context for JSON lists. Will insert/read commas before each item except
    // for the first one
    private inner class JSONListContext : JSONBaseContext() {
        private var first = true

        @Throws(TException::class)
        override suspend fun write() {
            if (first) {
                first = false
            } else {
                trans_.write(COMMA)
            }
        }

        @Throws(TException::class)
        override suspend fun read() {
            if (first) {
                first = false
            } else {
                readJSONSyntaxChar(COMMA)
            }
        }
    }

    // Context for JSON records. Will insert/read colons before the value portion
    // of each record pair, and commas before each key except the first. In
    // addition, will indicate that numbers in the key position need to be
    // escaped in quotes (since JSON keys must be strings).
    protected inner class JSONPairContext : JSONBaseContext() {
        private var first_ = true
        private var colon_ = true

        @Throws(TException::class)
        override suspend fun write() {
            if (first_) {
                first_ = false
                colon_ = true
            } else {
                trans_.write(if (colon_) COLON else COMMA)
                colon_ = !colon_
            }
        }

        @Throws(TException::class)
        override suspend fun read() {
            if (first_) {
                first_ = false
                colon_ = true
            } else {
                readJSONSyntaxChar(if (colon_) COLON else COMMA)
                colon_ = !colon_
            }
        }

        override fun escapeNum(): Boolean {
            return colon_
        }
    }

    // Holds up to one byte from the transport
    protected inner class LookaheadReader {
        private var hasData_ = false
        private val data_ = ByteArray(1)

        // Return and consume the next byte to be read, either taking it from the
        // data buffer if present or getting it from the transport otherwise.
        @Throws(TException::class)
        suspend fun read(): Byte {
            if (hasData_) {
                hasData_ = false
            } else {
                trans_.readAll(data_, 0, 1)
            }
            return data_[0]
        }

        // Return the next byte to be read without consuming, filling the data
        // buffer if it has not been filled already.
        @Throws(TException::class)
        suspend fun peek(): Byte {
            if (!hasData_) {
                trans_.readAll(data_, 0, 1)
            }
            hasData_ = true
            return data_[0]
        }
    }

    // Stack of nested contexts that we may be in
    private val contextStack_ = Stack<JSONBaseContext>()

    // Current context that we are in
    private var context_: JSONBaseContext = JSONBaseContext()

    // Reader that manages a 1-byte buffer
    private var reader_: LookaheadReader = LookaheadReader()

    // Write out the TField names as a string instead of the default integer value
    private var fieldNamesAsString_ = false

    // Push a new JSON context onto the stack.
    private fun pushContext(c: JSONBaseContext) {
        contextStack_.push(context_)
        context_ = c
    }

    // Pop the last JSON context off the stack
    private fun popContext() {
        context_ = contextStack_.pop()
    }

    // Reset the context stack to its initial state
    private fun resetContext() {
        while (!contextStack_.isEmpty()) {
            popContext()
        }
    }

    /**
     * Constructor
     */
    constructor(trans: TTransport, fieldNamesAsString: Boolean) : this(trans) {
        fieldNamesAsString_ = fieldNamesAsString
    }

    override fun reset() {
        contextStack_.clear()
        context_ = JSONBaseContext()
        reader_ = LookaheadReader()
    }

    // Temporary buffer used by several methods
    private val tmpbuf_ = ByteArray(4)

    // Read a byte that must match b[0]; otherwise an exception is thrown.
    // Marked protected to avoid synthetic accessor in JSONListContext.read
    // and JSONPairContext.read
    @Throws(TException::class)
    protected suspend fun readJSONSyntaxChar(b: ByteArray) {
        val ch = reader_.read()
        if (ch != b[0]) {
            throw TProtocolException(
                TProtocolException.INVALID_DATA,
                "Unexpected character:" + ch.toInt().toChar()
            )
        }
    }

    // Write the bytes in array buf as a JSON characters, escaping as needed
    @Throws(TException::class)
    private suspend fun writeJSONString(b: ByteArray) {
        context_.write()
        trans_.write(QUOTE)
        val len = b.size
        for (i in 0 until len) {
            if (b[i] and 0x00FF >= 0x30) {
                if (b[i] == BACKSLASH[0]) {
                    trans_.write(BACKSLASH)
                    trans_.write(BACKSLASH)
                } else {
                    trans_.write(b, i, 1)
                }
            } else {
                tmpbuf_[0] = JSON_CHAR_TABLE[b[i].toInt()]
                if (tmpbuf_[0] == 1.toByte()) {
                    trans_.write(b, i, 1)
                } else if (tmpbuf_[0] > 1) {
                    trans_.write(BACKSLASH)
                    trans_.write(tmpbuf_, 0, 1)
                } else {
                    trans_.write(ESCSEQ)
                    tmpbuf_[0] = hexChar((b[i] shr 4))
                    tmpbuf_[1] = hexChar(b[i])
                    trans_.write(tmpbuf_, 0, 2)
                }
            }
        }
        trans_.write(QUOTE)
    }

    // Write out number as a JSON value. If the context dictates so, it will be
    // wrapped in quotes to output as a JSON string.
    @Throws(TException::class)
    private suspend fun writeJSONInteger(num: Long) {
        context_.write()
        val str = java.lang.Long.toString(num)
        val escapeNum = context_.escapeNum()
        if (escapeNum) {
            trans_.write(QUOTE)
        }
        val buf = str.toByteArray(StandardCharsets.UTF_8)
        trans_.write(buf)
        if (escapeNum) {
            trans_.write(QUOTE)
        }
    }

    // Write out a double as a JSON value. If it is NaN or infinity or if the
    // context dictates escaping, write out as JSON string.
    @Throws(TException::class)
    private suspend fun writeJSONDouble(num: Double) {
        context_.write()
        val str = java.lang.Double.toString(num)
        var special = false
        when (str[0]) {
            'N', 'I' -> special = true
            '-' -> if (str[1] == 'I') { // -Infinity
                special = true
            }
            else -> {
            }
        }
        val escapeNum = special || context_.escapeNum()
        if (escapeNum) {
            trans_.write(QUOTE)
        }
        val b = str.toByteArray(StandardCharsets.UTF_8)
        trans_.write(b, 0, b.size)
        if (escapeNum) {
            trans_.write(QUOTE)
        }
    }

    // Write out contents of byte array b as a JSON string with base-64 encoded
    // data
    @Throws(TException::class)
    private suspend fun writeJSONBase64(b: ByteArray, offset: Int, length: Int) {
        context_.write()
        trans_.write(QUOTE)
        var len = length
        var off = offset
        while (len >= 3) {
            // Encode 3 bytes at a time
            TBase64Utils.encode(b, off, 3, tmpbuf_, 0)
            trans_.write(tmpbuf_, 0, 4)
            off += 3
            len -= 3
        }
        if (len > 0) {
            // Encode remainder
            TBase64Utils.encode(b, off, len, tmpbuf_, 0)
            trans_.write(tmpbuf_, 0, len + 1)
        }
        trans_.write(QUOTE)
    }

    @Throws(TException::class)
    private suspend fun writeJSONObjectStart() {
        context_.write()
        trans_.write(LBRACE)
        pushContext(JSONPairContext())
    }

    @Throws(TException::class)
    private suspend fun writeJSONObjectEnd() {
        popContext()
        trans_.write(RBRACE)
    }

    @Throws(TException::class)
    private suspend fun writeJSONArrayStart() {
        context_.write()
        trans_.write(LBRACKET)
        pushContext(JSONListContext())
    }

    @Throws(TException::class)
    private suspend fun writeJSONArrayEnd() {
        popContext()
        trans_.write(RBRACKET)
    }

    @Throws(TException::class)
    override suspend fun writeMessageBegin(message: TMessage?) {
        resetContext() // THRIFT-3743
        writeJSONArrayStart()
        writeJSONInteger(VERSION)
        val b = message!!.name!!.toByteArray(StandardCharsets.UTF_8)
        writeJSONString(b)
        writeJSONInteger(message.type.toLong())
        writeJSONInteger(message.seqid.toLong())
    }

    @Throws(TException::class)
    override suspend fun writeMessageEnd() {
        writeJSONArrayEnd()
    }

    @Throws(TException::class)
    override suspend fun writeStructBegin(struct: TStruct?) {
        writeJSONObjectStart()
    }

    @Throws(TException::class)
    override suspend fun writeStructEnd() {
        writeJSONObjectEnd()
    }

    @Throws(TException::class)
    override suspend fun writeFieldBegin(field: TField?) {
        if (fieldNamesAsString_) {
            writeString(field!!.name)
        } else {
            writeJSONInteger(field!!.id.toLong())
        }
        writeJSONObjectStart()
        writeJSONString(getTypeNameForTypeID(field.type))
    }

    @Throws(TException::class)
    override suspend fun writeFieldEnd() {
        writeJSONObjectEnd()
    }

    override suspend fun writeFieldStop() {}

    @Throws(TException::class)
    override suspend fun writeMapBegin(map: TMap?) {
        writeJSONArrayStart()
        writeJSONString(getTypeNameForTypeID(map!!.keyType))
        writeJSONString(getTypeNameForTypeID(map.valueType))
        writeJSONInteger(map.size.toLong())
        writeJSONObjectStart()
    }

    @Throws(TException::class)
    override suspend fun writeMapEnd() {
        writeJSONObjectEnd()
        writeJSONArrayEnd()
    }

    @Throws(TException::class)
    override suspend fun writeListBegin(list: TList?) {
        writeJSONArrayStart()
        writeJSONString(getTypeNameForTypeID(list!!.elemType))
        writeJSONInteger(list.size.toLong())
    }

    @Throws(TException::class)
    override suspend fun writeListEnd() {
        writeJSONArrayEnd()
    }

    @Throws(TException::class)
    override suspend fun writeSetBegin(set: TSet?) {
        writeJSONArrayStart()
        writeJSONString(getTypeNameForTypeID(set!!.elemType))
        writeJSONInteger(set.size.toLong())
    }

    @Throws(TException::class)
    override suspend fun writeSetEnd() {
        writeJSONArrayEnd()
    }

    @Throws(TException::class)
    override suspend fun writeBool(b: Boolean) {
        writeJSONInteger(if (b) 1.toLong() else 0.toLong())
    }

    @Throws(TException::class)
    override suspend fun writeByte(b: Byte) {
        writeJSONInteger(b.toLong())
    }

    @Throws(TException::class)
    override suspend fun writeI16(i16: Short) {
        writeJSONInteger(i16.toLong())
    }

    @Throws(TException::class)
    override suspend fun writeI32(i32: Int) {
        writeJSONInteger(i32.toLong())
    }

    @Throws(TException::class)
    override suspend fun writeI64(i64: Long) {
        writeJSONInteger(i64)
    }

    @Throws(TException::class)
    override suspend fun writeDouble(dub: Double) {
        writeJSONDouble(dub)
    }

    @Throws(TException::class)
    override suspend fun writeString(str: String?) {
        val b = str!!.toByteArray(StandardCharsets.UTF_8)
        writeJSONString(b)
    }

    @Throws(TException::class)
    override suspend fun writeBinary(buf: ByteBuffer?) {
        writeJSONBase64(
                buf!!.array(),
                buf.position() + buf.arrayOffset(),
                buf.limit() - buf.position() - buf.arrayOffset()
        )
    }

    /**
     * Reading methods.
     */
    // Read in a JSON string, unescaping as appropriate.. Skip reading from the
    // context if skipContext is true.
    @Throws(TException::class)
    private suspend fun readJSONString(skipContext: Boolean): TByteArrayOutputStream = withContext(Dispatchers.IO) {
        val arr = TByteArrayOutputStream(DEF_STRING_SIZE)
        val codeunits = ArrayList<Char>()
        if (!skipContext) {
            context_.read()
        }
        readJSONSyntaxChar(QUOTE)
        while (true) {
            var ch = reader_.read()
            if (ch == QUOTE[0]) {
                break
            }
            if (ch == ESCSEQ[0]) {
                ch = reader_.read()
                ch = if (ch == ESCSEQ[1]) {
                    trans_.readAll(tmpbuf_, 0, 4)
                    val cu = ((hexVal(tmpbuf_[0]).toShort() shl 12) +
                            (hexVal(tmpbuf_[1]).toShort() shl 8) +
                            (hexVal(tmpbuf_[2]).toShort() shl 4) +
                            hexVal(tmpbuf_[3]).toShort()).toShort()
                    try {
                        if (Character.isHighSurrogate(cu.toInt().toChar())) {
                            if (codeunits.size > 0) {
                                throw TProtocolException(
                                        TProtocolException.INVALID_DATA,
                                        "Expected low surrogate char"
                                )
                            }
                            codeunits.add(cu.toInt().toChar())
                        } else if (Character.isLowSurrogate(cu.toInt().toChar())) {
                            if (codeunits.size == 0) {
                                throw TProtocolException(
                                        TProtocolException.INVALID_DATA,
                                        "Expected high surrogate char"
                                )
                            }
                            codeunits.add(cu.toInt().toChar())
                            arr.write(
                                    String(
                                        intArrayOf(codeunits[0].code, codeunits[1].code),
                                            0, 2
                                    ).toByteArray(StandardCharsets.UTF_8)
                            )
                            codeunits.clear()
                        } else {
                            arr.write(
                                    String(intArrayOf(cu.toInt()), 0, 1)
                                            .toByteArray(StandardCharsets.UTF_8)
                            )
                        }
                        continue
                    } catch (ex: IOException) {
                        throw TProtocolException(
                                TProtocolException.INVALID_DATA,
                                "Invalid unicode sequence"
                        )
                    }
                } else {
                    val off = ESCAPE_CHARS.indexOf(ch.toInt().toChar())
                    if (off == -1) {
                        throw TProtocolException(
                                TProtocolException.INVALID_DATA,
                                "Expected control char"
                        )
                    }
                    ESCAPE_CHAR_VALS[off]
                }
            }
            arr.write(ch.toInt())
        }
        return@withContext arr
    }

    // Return true if the given byte could be a valid part of a JSON number.
    private fun isJSONNumeric(b: Byte): Boolean {
        when (b.toInt().toChar()) {
            '+', '-', '.', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'E', 'e' -> return true
        }
        return false
    }

    // Read in a sequence of characters that are all valid in JSON numbers. Does
    // not do a complete regex check to validate that this is actually a number.
    @Throws(TException::class)
    private suspend fun readJSONNumericChars(): String {
        val strbld = StringBuilder()
        while (true) {
            val ch = reader_.peek()
            if (!isJSONNumeric(ch)) {
                break
            }
            strbld.append(reader_.read().toInt().toChar())
        }
        return strbld.toString()
    }

    // Read in a JSON number. If the context dictates, read in enclosing quotes.
    @Throws(TException::class)
    private suspend fun readJSONInteger(): Long {
        context_.read()
        if (context_.escapeNum()) {
            readJSONSyntaxChar(QUOTE)
        }
        val str = readJSONNumericChars()
        if (context_.escapeNum()) {
            readJSONSyntaxChar(QUOTE)
        }
        return try {
            java.lang.Long.valueOf(str)
        } catch (ex: NumberFormatException) {
            throw TProtocolException(
                    TProtocolException.INVALID_DATA,
                    "Bad data encounted in numeric data"
            )
        }
    }

    // Read in a JSON double value. Throw if the value is not wrapped in quotes
    // when expected or if wrapped in quotes when not expected.
    @Throws(TException::class)
    private suspend fun readJSONDouble(): Double {
        context_.read()
        return if (reader_.peek() == QUOTE[0]) {
            val arr: TByteArrayOutputStream = readJSONString(true)
            val dub: Double = java.lang.Double.valueOf(arr.toString(StandardCharsets.UTF_8))
            if (!context_.escapeNum() && !java.lang.Double.isNaN(dub)
                    && !java.lang.Double.isInfinite(dub)
            ) {
                // Throw exception -- we should not be in a string in this case
                throw TProtocolException(
                        TProtocolException.INVALID_DATA,
                        "Numeric data unexpectedly quoted"
                )
            }
            dub
        } else {
            if (context_.escapeNum()) {
                // This will throw - we should have had a quote if escapeNum == true
                readJSONSyntaxChar(QUOTE)
            }
            try {
                java.lang.Double.valueOf(readJSONNumericChars())
            } catch (ex: NumberFormatException) {
                throw TProtocolException(
                        TProtocolException.INVALID_DATA,
                        "Bad data encounted in numeric data"
                )
            }
        }
    }

    // Read in a JSON string containing base-64 encoded data and decode it.
    @Throws(TException::class)
    private suspend fun readJSONBase64(): ByteArray {
        val arr: TByteArrayOutputStream = readJSONString(false)
        val b: ByteArray = arr.get()
        var len: Int = arr.len()
        var off = 0
        var size = 0
        // Ignore padding
        val bound = if (len >= 2) len - 2 else 0
        var i = len - 1
        while (i >= bound && b[i] == '='.code.toByte()) {
            --len
            --i
        }
        while (len >= 4) {
            // Decode 4 bytes at a time
            TBase64Utils.decode(b, off, 4, b, size) // NB: decoded in place
            off += 4
            len -= 4
            size += 3
        }
        // Don't decode if we hit the end or got a single leftover byte (invalid
        // base64 but legal for skip of regular string type)
        if (len > 1) {
            // Decode remainder
            TBase64Utils.decode(b, off, len, b, size) // NB: decoded in place
            size += len - 1
        }
        // Sadly we must copy the byte[] (any way around this?)
        val result = ByteArray(size)
        System.arraycopy(b, 0, result, 0, size)
        return result
    }

    @Throws(TException::class)
    private suspend fun readJSONObjectStart() {
        context_.read()
        readJSONSyntaxChar(LBRACE)
        pushContext(JSONPairContext())
    }

    @Throws(TException::class)
    private suspend fun readJSONObjectEnd() {
        readJSONSyntaxChar(RBRACE)
        popContext()
    }

    @Throws(TException::class)
    private suspend fun readJSONArrayStart() {
        context_.read()
        readJSONSyntaxChar(LBRACKET)
        pushContext(JSONListContext())
    }

    @Throws(TException::class)
    private suspend fun readJSONArrayEnd() {
        readJSONSyntaxChar(RBRACKET)
        popContext()
    }

    @Throws(TException::class)
    override suspend fun readMessageBegin(): TMessage {
        resetContext() // THRIFT-3743
        readJSONArrayStart()
        if (readJSONInteger() != VERSION) {
            throw TProtocolException(
                    TProtocolException.BAD_VERSION,
                    "Message contained bad version."
            )
        }
        val name: String = readJSONString(false).toString(StandardCharsets.UTF_8)
        val type = readJSONInteger().toByte()
        val seqid = readJSONInteger().toInt()
        return TMessage(name, type, seqid)
    }

    @Throws(TException::class)
    override suspend fun readMessageEnd() {
        readJSONArrayEnd()
    }

    @Throws(TException::class)
    override suspend fun readStructBegin(): TStruct {
        readJSONObjectStart()
        return ANONYMOUS_STRUCT
    }

    @Throws(TException::class)
    override suspend fun readStructEnd() {
        readJSONObjectEnd()
    }

    @Throws(TException::class)
    override suspend fun readFieldBegin(): TField {
        val ch = reader_.peek()
        val type: Byte
        var id: Short = 0
        if (ch == RBRACE[0]) {
            type = TType.STOP
        } else {
            id = readJSONInteger().toShort()
            readJSONObjectStart()
            type = getTypeIDForTypeName(readJSONString(false).get())
        }
        return TField("", type, id)
    }

    @Throws(TException::class)
    override suspend fun readFieldEnd() {
        readJSONObjectEnd()
    }

    @Throws(TException::class)
    override suspend fun readMapBegin(): TMap {
        readJSONArrayStart()
        val keyType = getTypeIDForTypeName(readJSONString(false).get())
        val valueType = getTypeIDForTypeName(readJSONString(false).get())
        val size = readJSONInteger().toInt()
        readJSONObjectStart()
        val map = TMap(keyType, valueType, size)
        checkReadBytesAvailable(map)
        return map
    }

    @Throws(TException::class)
    override suspend fun readMapEnd() {
        readJSONObjectEnd()
        readJSONArrayEnd()
    }

    @Throws(TException::class)
    override suspend fun readListBegin(): TList {
        readJSONArrayStart()
        val elemType = getTypeIDForTypeName(readJSONString(false).get())
        val size = readJSONInteger().toInt()
        val list = TList(elemType, size)
        checkReadBytesAvailable(list)
        return list
    }

    @Throws(TException::class)
    override suspend fun readListEnd() {
        readJSONArrayEnd()
    }

    @Throws(TException::class)
    override suspend fun readSetBegin(): TSet {
        readJSONArrayStart()
        val elemType = getTypeIDForTypeName(readJSONString(false).get())
        val size = readJSONInteger().toInt()
        val set = TSet(elemType, size)
        checkReadBytesAvailable(set)
        return set
    }

    @Throws(TException::class)
    override suspend fun readSetEnd() {
        readJSONArrayEnd()
    }

    @Throws(TException::class)
    override suspend fun readBool(): Boolean {
        return readJSONInteger() != 0L
    }

    @Throws(TException::class)
    override suspend fun readByte(): Byte {
        return readJSONInteger().toByte()
    }

    @Throws(TException::class)
    override suspend fun readI16(): Short {
        return readJSONInteger().toShort()
    }

    @Throws(TException::class)
    override suspend fun readI32(): Int {
        return readJSONInteger().toInt()
    }

    @Throws(TException::class)
    override suspend fun readI64(): Long {
        return readJSONInteger()
    }

    @Throws(TException::class)
    override suspend fun readDouble(): Double {
        return readJSONDouble()
    }

    @Throws(TException::class)
    override suspend fun readString(): String {
        return readJSONString(false).toString(StandardCharsets.UTF_8)
    }

    @Throws(TException::class)
    override suspend fun readBinary(): ByteBuffer {
        return ByteBuffer.wrap(readJSONBase64())
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
            2 -> 1 // Bool
            3 -> 1 // Byte
            4 -> 1 // Double
            6 -> 1 // I16
            8 -> 1 // I32
            10 -> 1 // I64
            11 -> 2 // string length
            12 -> 2 // empty struct
            13 -> 2 // element count Map
            14 -> 2 // element count Set
            15 -> 2 // element count List
            else -> throw TTransportException(TTransportException.UNKNOWN, "unrecognized type code")
        }
    }

    companion object {
        private val COMMA = byteArrayOf(','.code.toByte())
        private val COLON = byteArrayOf(':'.code.toByte())
        private val LBRACE = byteArrayOf('{'.code.toByte())
        private val RBRACE = byteArrayOf('}'.code.toByte())
        private val LBRACKET = byteArrayOf('['.code.toByte())
        private val RBRACKET = byteArrayOf(']'.code.toByte())
        private val QUOTE = byteArrayOf('"'.code.toByte())
        private val BACKSLASH = byteArrayOf('\\'.code.toByte())
        private val ZERO = byteArrayOf('0'.code.toByte())
        private val ESCSEQ = byteArrayOf('\\'.code.toByte(), 'u'.code.toByte(), '0'.code.toByte(), '0'.code.toByte())
        private const val VERSION: Long = 1
        private val JSON_CHAR_TABLE = byteArrayOf( /*  0   1   2   3   4   5   6   7   8   9   A   B   C   D   E   F */
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            'b'.code.toByte(),
            't'.code.toByte(),
            'n'.code.toByte(),
            0,
            'f'.code.toByte(),
            'r'.code.toByte(),
            0,
            0,  // 0
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,  // 1
            1,
            1,
            '"'.code.toByte(),
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1
        )
        private const val ESCAPE_CHARS = "\"\\/bfnrt"
        private val ESCAPE_CHAR_VALS = byteArrayOf(
            '"'.code.toByte(),
            '\\'.code.toByte(),
            '/'.code.toByte(),
            '\b'.code.toByte(),
            0x0C.toByte(), // form feed
            '\n'.code.toByte(),
            '\r'.code.toByte(),
            '\t'.code.toByte()
        )
        private const val DEF_STRING_SIZE = 16
        private val NAME_BOOL = byteArrayOf('t'.code.toByte(), 'f'.code.toByte())
        private val NAME_BYTE = byteArrayOf('i'.code.toByte(), '8'.code.toByte())
        private val NAME_I16 = byteArrayOf('i'.code.toByte(), '1'.code.toByte(), '6'.code.toByte())
        private val NAME_I32 = byteArrayOf('i'.code.toByte(), '3'.code.toByte(), '2'.code.toByte())
        private val NAME_I64 = byteArrayOf('i'.code.toByte(), '6'.code.toByte(), '4'.code.toByte())
        private val NAME_DOUBLE = byteArrayOf('d'.code.toByte(), 'b'.code.toByte(), 'l'.code.toByte())
        private val NAME_STRUCT = byteArrayOf('r'.code.toByte(), 'e'.code.toByte(), 'c'.code.toByte())
        private val NAME_STRING = byteArrayOf('s'.code.toByte(), 't'.code.toByte(), 'r'.code.toByte())
        private val NAME_MAP = byteArrayOf('m'.code.toByte(), 'a'.code.toByte(), 'p'.code.toByte())
        private val NAME_LIST = byteArrayOf('l'.code.toByte(), 's'.code.toByte(), 't'.code.toByte())
        private val NAME_SET = byteArrayOf('s'.code.toByte(), 'e'.code.toByte(), 't'.code.toByte())
        private val ANONYMOUS_STRUCT = TStruct()

        @Throws(TException::class)
        private fun getTypeNameForTypeID(typeID: Byte): ByteArray {
            return when (typeID) {
                TType.BOOL -> NAME_BOOL
                TType.BYTE -> NAME_BYTE
                TType.I16 -> NAME_I16
                TType.I32 -> NAME_I32
                TType.I64 -> NAME_I64
                TType.DOUBLE -> NAME_DOUBLE
                TType.STRING -> NAME_STRING
                TType.STRUCT -> NAME_STRUCT
                TType.MAP -> NAME_MAP
                TType.SET -> NAME_SET
                TType.LIST -> NAME_LIST
                else -> throw TProtocolException(
                        TProtocolException.NOT_IMPLEMENTED,
                        "Unrecognized type"
                )
            }
        }

        @Throws(TException::class)
        private fun getTypeIDForTypeName(name: ByteArray): Byte {
            var result = TType.STOP
            if (name.size > 1) {
                when (name[0].toInt().toChar()) {
                    'd' -> result = TType.DOUBLE
                    'i' -> when (name[1].toInt().toChar()) {
                        '8' -> result = TType.BYTE
                        '1' -> result = TType.I16
                        '3' -> result = TType.I32
                        '6' -> result = TType.I64
                    }
                    'l' -> result = TType.LIST
                    'm' -> result = TType.MAP
                    'r' -> result = TType.STRUCT
                    's' -> if (name[1] == 't'.code.toByte()) {
                        result = TType.STRING
                    } else if (name[1] == 'e'.code.toByte()) {
                        result = TType.SET
                    }
                    't' -> result = TType.BOOL
                }
            }
            if (result == TType.STOP) {
                throw TProtocolException(
                        TProtocolException.NOT_IMPLEMENTED,
                        "Unrecognized type"
                )
            }
            return result
        }

        // Convert a byte containing a hex char ('0'-'9' or 'a'-'f') into its
        // corresponding hex value
        @Throws(TException::class)
        private fun hexVal(ch: Byte): Byte {
            return if (ch >= '0'.code.toByte() && ch <= '9'.code.toByte()) {
                (ch.toInt().toChar() - '0').toByte()
            } else if (ch >= 'a'.code.toByte() && ch <= 'f'.code.toByte()) {
                (ch.toInt().toChar() - 'a' + 10).toByte()
            } else {
                throw TProtocolException(
                        TProtocolException.INVALID_DATA,
                        "Expected hex character"
                )
            }
        }

        // Convert a byte containing a hex value to its corresponding hex character
        private fun hexChar(hex: Byte): Byte {
            var char = hex
            char = char and 0x0F
            return if (char < 10) {
                (char.toInt().toChar() + '0').code.toByte()
            } else {
                ((char - 10).toChar().plus('a')).code.toByte()
            }
        }
    }
}
