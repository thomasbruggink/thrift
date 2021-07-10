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

import org.apache.thrift.TConfiguration

class TMemoryInputTransport constructor(
    _configuration: TConfiguration = TConfiguration(),
    buf: ByteArray = ByteArray(0),
    offset: Int = 0,
    length: Int = buf.size
) : TEndpointTransport(_configuration) {
    override var buffer: ByteArray? = null
        private set
    override var bufferPosition = 0
        private set
    private var endPos_ = 0

    constructor(buf: ByteArray) : this(TConfiguration(), buf) {}
    constructor(buf: ByteArray, offset: Int, length: Int) : this(TConfiguration(), buf, offset, length) {}

    fun reset(buf: ByteArray, offset: Int = 0, length: Int = buf.size) {
        buffer = buf
        bufferPosition = offset
        endPos_ = offset + length
        try {
            resetConsumedMessageSize(-1)
        } catch (e: TTransportException) {
            // ignore
        }
    }

    fun clear() {
        buffer = null
        try {
            resetConsumedMessageSize(-1)
        } catch (e: TTransportException) {
            // ignore
        }
    }

    override suspend fun close() {}
    override val isOpen: Boolean
        get() = true

    @Throws(TTransportException::class)
    override suspend fun open() {
    }

    @Throws(TTransportException::class)
    override suspend fun read(buf: ByteArray, off: Int, len: Int): Int {
        val bytesRemaining = bytesRemainingInBuffer
        val amtToRead = if (len > bytesRemaining) bytesRemaining else len
        if (amtToRead > 0) {
            System.arraycopy(buffer as Any, bufferPosition, buf, off, amtToRead)
            consumeBuffer(amtToRead)
            countConsumedMessageBytes(amtToRead.toLong())
        }
        return amtToRead
    }

    @Throws(TTransportException::class)
    override suspend fun write(buf: ByteArray, off: Int, len: Int) {
        throw UnsupportedOperationException("No writing allowed!")
    }

    override val bytesRemainingInBuffer: Int
        get() = endPos_ - bufferPosition

    override suspend fun consumeBuffer(len: Int) {
        bufferPosition += len
    }

    init {
        reset(buf, offset, length)
        updateKnownMessageSize(length.toLong())
    }
}
