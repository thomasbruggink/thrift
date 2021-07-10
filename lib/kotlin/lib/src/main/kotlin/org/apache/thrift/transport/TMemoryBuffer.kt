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
package org.apache.thrift.transport

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.apache.thrift.TByteArrayOutputStream
import org.apache.thrift.TConfiguration
import org.apache.thrift.andToInt
import java.nio.charset.Charset

/**
 * Memory buffer-based implementation of the TTransport interface.
 */
class TMemoryBuffer : TEndpointTransport {
    /**
     * Create a TMemoryBuffer with an initial buffer size of *size*. The
     * internal buffer will grow as necessary to accommodate the size of the data
     * being written to it.
     *
     * @param size the initial size of the buffer
     */
    constructor(size: Int) : super(TConfiguration()) {
        arr_ = TByteArrayOutputStream(size)
        updateKnownMessageSize(size.toLong())
    }

    /**
     * Create a TMemoryBuffer with an initial buffer size of *size*. The
     * internal buffer will grow as necessary to accommodate the size of the data
     * being written to it.
     *
     * @param config
     * @param size the initial size of the buffer
     */
    constructor(config: TConfiguration, size: Int) : super(config) {
        arr_ = TByteArrayOutputStream(size)
        updateKnownMessageSize(size.toLong())
    }

    override val isOpen: Boolean
        get() = true

    override suspend fun open() {
        /* Do nothing */
    }

    override suspend fun close() {
        /* Do nothing */
    }

    @Throws(TTransportException::class)
    override suspend fun read(buf: ByteArray, off: Int, len: Int): Int {
        checkReadBytesAvailable(len.toLong())
        val src = arr_.get()
        val amtToRead = if (len > arr_.len() - pos_) arr_.len() - pos_ else len
        if (amtToRead > 0) {
            System.arraycopy(src, pos_, buf, off, amtToRead)
            pos_ += amtToRead
        }
        return amtToRead
    }

    override suspend fun write(buf: ByteArray, off: Int, len: Int) = withContext(Dispatchers.IO) {
        arr_.write(buf, off, len)
    }

    /**
     * Output the contents of the memory buffer as a String, using the supplied
     * encoding
     * @param charset the encoding to use
     * @return the contents of the memory buffer as a String
     */
    fun toString(charset: Charset?): String {
        return arr_.toString(charset!!)
    }

    fun inspect(): String {
        val buf = StringBuilder()
        val bytes = arr_.toByteArray()
        for (i in bytes.indices) {
            buf.append(if (pos_ == i) "==>" else "").append(Integer.toHexString(bytes[i] andToInt 0xff)).append(" ")
        }
        return buf.toString()
    }

    // The contents of the buffer
    private var arr_: TByteArrayOutputStream

    // Position to read next byte from
    private var pos_ = 0
    fun length(): Int {
        return arr_.size()
    }

    val array: ByteArray
        get() = arr_.get()
}
