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
 * TTransport for writing to an AutoExpandingBuffer.
 */
class AutoExpandingBufferWriteTransport(config: TConfiguration?, initialCapacity: Int, frontReserve: Int) :
    TEndpointTransport(config) {
    val buf: AutoExpandingBuffer

    /**
     * @return length of the buffer, including any front reserve
     */
    var length: Int
        private set
    private val res: Int
    override suspend fun close() {}
    override val isOpen: Boolean
        get() = true

    @Throws(TTransportException::class)
    override suspend fun open() {
    }

    @Throws(TTransportException::class)
    override suspend fun read(buf: ByteArray, off: Int, len: Int): Int {
        throw UnsupportedOperationException()
    }

    @Throws(TTransportException::class)
    override suspend fun write(buf: ByteArray, off: Int, len: Int) {
        this.buf.resizeIfNecessary(length + len)
        System.arraycopy(buf, off, this.buf.array(), length, len)
        length += len
    }

    fun reset() {
        length = res
    }

    /**
     * Constructor.
     * @param initialCapacity the initial capacity of the buffer
     * @param frontReserve space, if any, to reserve at the beginning such
     * that the first write is after this reserve.
     * This allows framed transport to reserve space
     * for the frame buffer length.
     * @throws IllegalArgumentException if initialCapacity is less than one
     * @throws IllegalArgumentException if frontReserve is less than zero
     * @throws IllegalArgumentException if frontReserve is greater than initialCapacity
     */
    init {
        require(initialCapacity >= 1) { "initialCapacity" }
        require(!(frontReserve < 0 || initialCapacity < frontReserve)) { "frontReserve" }
        buf = AutoExpandingBuffer(initialCapacity)
        length = frontReserve
        res = frontReserve
    }
}
