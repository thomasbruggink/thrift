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

import org.apache.thrift.and
import org.apache.thrift.shl
import org.apache.thrift.shr
import kotlin.experimental.or
import org.apache.thrift.get

/**
 * Class for encoding and decoding Base64 data.
 *
 * This class is kept at package level because the interface does no input
 * validation and is therefore too low-level for generalized reuse.
 *
 * Note also that the encoding does not pad with equal signs , as discussed in
 * section 2.2 of the RFC (http://www.faqs.org/rfcs/rfc3548.html). Furthermore,
 * bad data encountered when decoding is neither rejected or ignored but simply
 * results in bad decoded data -- this is not in compliance with the RFC but is
 * done in the interest of performance.
 *
 */
internal object TBase64Utils {
    private const val ENCODE_TABLE = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"

    /**
     * Encode len bytes of data in src at offset srcOff, storing the result into
     * dst at offset dstOff. len must be 1, 2, or 3. dst must have at least len+1
     * bytes of space at dstOff. src and dst should not be the same object. This
     * method does no validation of the input values in the interest of
     * performance.
     *
     * @param src  the source of bytes to encode
     * @param srcOff  the offset into the source to read the unencoded bytes
     * @param len  the number of bytes to encode (must be 1, 2, or 3).
     * @param dst  the destination for the encoding
     * @param dstOff  the offset into the destination to place the encoded bytes
     */
    fun encode(
        src: ByteArray, srcOff: Int, len: Int, dst: ByteArray,
        dstOff: Int
    ) {
        dst[dstOff] = ENCODE_TABLE[src[srcOff] shr 2 and 0x3F].code.toByte()
        if (len == 3) {
            dst[dstOff + 1] = ENCODE_TABLE[src[srcOff] shl 4 and 0x30 or (src[srcOff + 1] shr 4 and 0x0F)].code.toByte()
            dst[dstOff + 2] =
                ENCODE_TABLE[src[srcOff + 1] shl 2 and 0x3C or (src[srcOff + 2] shr 6 and 0x03)].code.toByte()
            dst[dstOff + 3] = ENCODE_TABLE[src[srcOff + 2] and 0x3F].code.toByte()
        } else if (len == 2) {
            dst[dstOff + 1] = ENCODE_TABLE[src[srcOff] shl 4 and 0x30 or (src[srcOff + 1] shr 4 and 0x0F)].code.toByte()
            dst[dstOff + 2] = ENCODE_TABLE[src[srcOff + 1] shl 2 and 0x3C].code.toByte()
        } else { // len == 1) {
            dst[dstOff + 1] = ENCODE_TABLE[src[srcOff] shl 4 and 0x30].code.toByte()
        }
    }

    private val DECODE_TABLE = byteArrayOf(
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 62, -1, -1, -1, 63,
        52, 53, 54, 55, 56, 57, 58, 59, 60, 61, -1, -1, -1, -1, -1, -1,
        -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
        15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, -1, -1, -1, -1, -1,
        -1, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
        41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1
    )

    /**
     * Decode len bytes of data in src at offset srcOff, storing the result into
     * dst at offset dstOff. len must be 2, 3, or 4. dst must have at least len-1
     * bytes of space at dstOff. src and dst may be the same object as long as
     * dstoff <= srcOff. This method does no validation of the input values in
     * the interest of performance.
     *
     * @param src  the source of bytes to decode
     * @param srcOff  the offset into the source to read the encoded bytes
     * @param len  the number of bytes to decode (must be 2, 3, or 4)
     * @param dst  the destination for the decoding
     * @param dstOff  the offset into the destination to place the decoded bytes
     */
    fun decode(
        src: ByteArray, srcOff: Int, len: Int, dst: ByteArray,
        dstOff: Int
    ) {
        dst[dstOff] = (DECODE_TABLE[(src[srcOff] and 0x0FF).toInt()] shl 2 or
                (DECODE_TABLE[(src[srcOff + 1] and 0x0FF).toInt()] shr 4)) as Byte
        if (len > 2) {
            dst[dstOff + 1] = (DECODE_TABLE[(src[srcOff + 1] and 0x0FF).toInt()] shl 4 and 0xF0 or
                    (DECODE_TABLE[(src[srcOff + 2] and 0x0FF).toInt()] shr 2)) as Byte
            if (len > 3) {
                dst[dstOff + 2] = (DECODE_TABLE[(src[srcOff + 2] and 0x0FF).toInt()] shl 6 and 0xC0 or
                        DECODE_TABLE[(src[srcOff + 3] and 0x0FF).toInt()]) as Byte
            }
        }
    }
}

