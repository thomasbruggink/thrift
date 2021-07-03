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

/**
 * TTransport for reading from an AutoExpandingBuffer.
 */
class AutoExpandingBufferReadTransport(config: TConfiguration?, initialCapacity: Int) :
    TEndpointTransport(config) {
    private val buf: AutoExpandingBuffer = AutoExpandingBuffer(initialCapacity)
    override var bufferPosition = 0
        private set
    private var limit = 0
    @Throws(TTransportException::class)
    suspend fun fill(inTrans: TTransport, length: Int) {
        buf.resizeIfNecessary(length)
        inTrans.readAll(buf.array(), 0, length)
        bufferPosition = 0
        limit = length
    }

    override suspend fun close() {}

    override val isOpen: Boolean
        get() = true

    @Throws(TTransportException::class)
    override suspend fun open() {
    }

    @Throws(TTransportException::class)
    override suspend fun read(buf: ByteArray, off: Int, len: Int): Int {
        val amtToRead = len.coerceAtMost(bytesRemainingInBuffer)
        if (amtToRead > 0) {
            System.arraycopy(this.buf.array(), bufferPosition, buf, off, amtToRead)
            consumeBuffer(amtToRead)
        }
        return amtToRead
    }

    @Throws(TTransportException::class)
    override suspend fun write(buf: ByteArray, off: Int, len: Int) {
        throw UnsupportedOperationException()
    }

    override suspend fun consumeBuffer(len: Int) {
        bufferPosition += len
    }

    override val buffer: ByteArray
        get() = buf.array()
    override val bytesRemainingInBuffer: Int
        get() = limit - bufferPosition

}
