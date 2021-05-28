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
import org.apache.thrift.scheme.IScheme
import org.apache.thrift.scheme.TupleScheme
import org.apache.thrift.transport.TTransport
import java.util.*
import kotlin.experimental.and
import kotlin.experimental.or

class TTupleProtocol(transport: TTransport?) : TCompactProtocol(transport) {
    class Factory : TProtocolFactory {
        override fun getProtocol(trans: TTransport?): TProtocol {
            return TTupleProtocol(trans)
        }
    }

    override val scheme: Class<out IScheme<*>?>
        get() = TupleScheme::class.java

    @Throws(TException::class)
    fun writeBitSet(bs: BitSet, vectorWidth: Int) {
        val bytes = toByteArray(bs, vectorWidth)
        for (b in bytes) {
            writeByte(b)
        }
    }

    @Throws(TException::class)
    fun readBitSet(i: Int): BitSet {
        val length = Math.ceil(i / 8.0).toInt()
        val bytes = ByteArray(length)
        for (j in 0 until length) {
            bytes[j] = readByte()
        }
        return fromByteArray(bytes)
    }

    @Throws(TException::class)
    fun readMapBegin(keyType: Byte, valTyep: Byte): TMap {
        val size = super.readI32()
        val map = TMap(keyType, valTyep, size)
        checkReadBytesAvailable(map)
        return map
    }

    @Throws(TException::class)
    fun readListBegin(type: Byte): TList {
        val size = super.readI32()
        val list = TList(type, size)
        checkReadBytesAvailable(list)
        return list
    }

    @Throws(TException::class)
    fun readSetBegin(type: Byte): TSet {
        return TSet(readListBegin(type))
    }

    @Throws(TException::class)
    override fun readMapEnd() {
    }

    @Throws(TException::class)
    override fun readListEnd() {
    }

    @Throws(TException::class)
    override fun readSetEnd() {
    }

    companion object {
        /**
         * Returns a bitset containing the values in bytes. The byte-ordering must be
         * big-endian.
         */
        fun fromByteArray(bytes: ByteArray): BitSet {
            val bits = BitSet()
            for (i in 0 until bytes.size * 8) {
                if (bytes[bytes.size - i / 8 - 1] and ((1 shl i).rem(8)).toByte() > 0) {
                    bits.set(i)
                }
            }
            return bits
        }

        /**
         * Returns a byte array of at least length 1. The most significant bit in the
         * result is guaranteed not to be a 1 (since BitSet does not support sign
         * extension). The byte-ordering of the result is big-endian which means the
         * most significant bit is in element 0. The bit at index 0 of the bit set is
         * assumed to be the least significant bit.
         *
         * @param bits
         * @param vectorWidth
         * @return a byte array of at least length 1
         */
        fun toByteArray(bits: BitSet, vectorWidth: Int): ByteArray {
            val bytes = ByteArray(Math.ceil(vectorWidth / 8.0).toInt())
            for (i in 0 until bits.length()) {
                if (bits[i]) {
                    bytes[bytes.size - i / 8 - 1] = bytes[bytes.size - i / 8 - 1] or ((1 shl i).rem(8)).toByte()
                }
            }
            return bytes
        }
    }
}
