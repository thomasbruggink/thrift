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
package org.apache.thrift

import kotlin.experimental.and
import kotlin.experimental.or

/**
 * Utility methods for use when encoding/decoding raw data as byte arrays.
 */
object EncodingUtils {
    /**
     * Encode `integer` as a series of 4 bytes into `buf`
     * starting at position `offset`.
     *
     * @param integer
     * The integer to encode.
     * @param buf
     * The buffer to write to.
     * @param offset
     * The offset within `buf` to start the encoding.
     */
    /**
     * Encode `integer` as a series of 4 bytes into `buf`
     * starting at position 0 within that buffer.
     *
     * @param integer
     * The integer to encode.
     * @param buf
     * The buffer to write to.
     */
    @JvmOverloads
    fun encodeBigEndian(integer: Int, buf: ByteArray, offset: Int = 0) {
        buf[offset] = (0xff and (integer shr 24)).toByte()
        buf[offset + 1] = (0xff and (integer shr 16)).toByte()
        buf[offset + 2] = (0xff and (integer shr 8)).toByte()
        buf[offset + 3] = (0xff and integer).toByte()
    }
    /**
     * Decode a series of 4 bytes from `buf`, start at
     * `offset`, and interpret them as an integer.
     *
     * @param buf
     * The buffer to read from.
     * @param offset
     * The offset with `buf` to start the decoding.
     * @return An integer, as read from the buffer.
     */
    /**
     * Decode a series of 4 bytes from `buf`, starting at position 0,
     * and interpret them as an integer.
     *
     * @param buf
     * The buffer to read from.
     * @return An integer, as read from the buffer.
     */
    @JvmOverloads
    fun decodeBigEndian(buf: ByteArray, offset: Int = 0): Int {
        return (buf[offset] and 0xff.toByte() shl 24 or (buf[offset + 1] and 0xff.toByte() shl 16)
                or (buf[offset + 2] and 0xff.toByte() shl 8) or (buf[offset + 3] and 0xff.toByte())).toInt()
    }

    /**
     * Bitfield utilities.
     * Returns true if the bit at position is set in v.
     */
    fun testBit(v: Byte, position: Int): Boolean {
        return testBit(v.toInt(), position)
    }

    fun testBit(v: Short, position: Int): Boolean {
        return testBit(v.toInt(), position)
    }

    fun testBit(v: Int, position: Int): Boolean {
        return v and (1 shl position) != 0
    }

    fun testBit(v: Long, position: Int): Boolean {
        return v and (1L shl position) != 0L
    }

    /**
     * Returns v, with the bit at position set to zero.
     */
    fun clearBit(v: Byte, position: Int): Byte {
        return clearBit(v.toInt(), position).toByte()
    }

    fun clearBit(v: Short, position: Int): Short {
        return clearBit(v.toInt(), position).toShort()
    }

    fun clearBit(v: Int, position: Int): Int {
        return v and (1 shl position).inv()
    }

    fun clearBit(v: Long, position: Int): Long {
        return v and (1L shl position).inv()
    }

    /**
     * Returns v, with the bit at position set to 1 or 0 depending on value.
     */
    fun setBit(v: Byte, position: Int, value: Boolean): Byte {
        return setBit(v.toInt(), position, value).toByte()
    }

    fun setBit(v: Short, position: Int, value: Boolean): Short {
        return setBit(v.toInt(), position, value).toShort()
    }

    fun setBit(v: Int, position: Int, value: Boolean): Int {
        return if (value) v or (1 shl position) else clearBit(v, position)
    }

    fun setBit(v: Long, position: Int, value: Boolean): Long {
        return if (value) v or (1L shl position) else clearBit(v, position)
    }
}

