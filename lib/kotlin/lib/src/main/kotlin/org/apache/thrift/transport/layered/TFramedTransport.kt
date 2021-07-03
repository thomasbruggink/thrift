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
package org.apache.thrift.transport.layered

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.apache.thrift.TByteArrayOutputStream
import org.apache.thrift.TConfiguration
import org.apache.thrift.andToInt
import org.apache.thrift.transport.TMemoryInputTransport
import org.apache.thrift.transport.TTransport
import org.apache.thrift.transport.TTransportException
import org.apache.thrift.transport.TTransportFactory
import java.util.*

/**
 * TFramedTransport is a buffered TTransport that ensures a fully read message
 * every time by preceding messages with a 4-byte frame size.
 */
class TFramedTransport @JvmOverloads constructor(
        transport: TTransport,
        maxLength: Int = TConfiguration.DEFAULT_MAX_FRAME_SIZE
) :
        TLayeredTransport(transport) {
    /**
     * Buffer for output
     */
    private val writeBuffer_ = TByteArrayOutputStream(1024)

    /**
     * Buffer for input
     */
    private val readBuffer_: TMemoryInputTransport

    class Factory : TTransportFactory {
        private var maxLength_: Int

        constructor() {
            maxLength_ = TConfiguration.DEFAULT_MAX_FRAME_SIZE
        }

        constructor(maxLength: Int) {
            maxLength_ = maxLength
        }

        @Throws(TTransportException::class)
        override fun getTransport(trans: TTransport): TTransport {
            return TFramedTransport(trans, maxLength_)
        }
    }

    @Throws(TTransportException::class)
    override suspend fun open() {
        innerTransport.open()
    }

    override val isOpen: Boolean
        get() = innerTransport.isOpen

    override suspend fun close() = withContext(Dispatchers.IO) {
        innerTransport.close()
    }

    @Throws(TTransportException::class)
    override suspend fun read(buf: ByteArray, off: Int, len: Int): Int {
        val got: Int = readBuffer_.read(buf, off, len)
        if (got > 0) {
            return got
        }

        // Read another frame of data
        readFrame()
        return readBuffer_.read(buf, off, len)
    }

    override val buffer: ByteArray?
        get() = readBuffer_.buffer
    override val bufferPosition: Int
        get() = readBuffer_.bufferPosition
    override val bytesRemainingInBuffer: Int
        get() = readBuffer_.bytesRemainingInBuffer

    override suspend fun consumeBuffer(len: Int) {
        readBuffer_.consumeBuffer(len)
    }

    fun clear() {
        readBuffer_.clear()
    }

    private val i32buf = ByteArray(4)

    @Throws(TTransportException::class)
    private suspend fun readFrame() {
        innerTransport.readAll(i32buf, 0, 4)
        val size = decodeFrameSize(i32buf)
        if (size < 0) {
            close()
            throw TTransportException(TTransportException.CORRUPTED_DATA, "Read a negative frame size ($size)!")
        }
        if (size > innerTransport.configuration.maxFrameSize) {
            close()
            throw TTransportException(
                    TTransportException.CORRUPTED_DATA,
                    "Frame size (" + size + ") larger than max length (" + innerTransport.configuration
                            .maxFrameSize + ")!"
            )
        }
        val buff = ByteArray(size)
        innerTransport.readAll(buff, 0, size)
        readBuffer_.reset(buff)
    }

    @Throws(TTransportException::class)
    override suspend fun write(buf: ByteArray, off: Int, len: Int) = withContext(Dispatchers.IO) {
        writeBuffer_.write(buf, off, len)
    }

    @Throws(TTransportException::class)
    override suspend fun flush() = withContext(Dispatchers.IO) {
        val buf = writeBuffer_.get()
        val len = writeBuffer_.len() - 4 // account for the prepended frame size
        writeBuffer_.reset()
        writeBuffer_.write(sizeFiller_, 0, 4) // make room for the next frame's size data
        encodeFrameSize(len, buf) // this is the frame length without the filler
        innerTransport.write(buf, 0, len + 4) // we have to write the frame size and frame data
        innerTransport.flush()
    }

    companion object {
        /**
         * Something to fill in the first four bytes of the buffer
         * to make room for the frame size.  This allows the
         * implementation to write once instead of twice.
         */
        private val sizeFiller_ = byteArrayOf(0x00, 0x00, 0x00, 0x00)
        fun encodeFrameSize(frameSize: Int, buf: ByteArray) {
            buf[0] = (0xff and (frameSize shr 24)).toByte()
            buf[1] = (0xff and (frameSize shr 16)).toByte()
            buf[2] = (0xff and (frameSize shr 8)).toByte()
            buf[3] = (0xff and frameSize).toByte()
        }

        fun decodeFrameSize(buf: ByteArray): Int {
            return buf[0] andToInt 0xff shl 24 or
                    (buf[1] andToInt 0xff shl 16) or
                    (buf[2] andToInt 0xff shl 8) or
                    (buf[3] andToInt 0xff)
        }
    }

    /**
     * Constructor wraps around another transport
     */
    init {
        val _configuration =
                if (Objects.isNull(transport.configuration)) TConfiguration() else transport.configuration
        _configuration.maxFrameSize = maxLength
        writeBuffer_.write(sizeFiller_, 0, 4)
        readBuffer_ = TMemoryInputTransport(_configuration, ByteArray(0))
    }
}
