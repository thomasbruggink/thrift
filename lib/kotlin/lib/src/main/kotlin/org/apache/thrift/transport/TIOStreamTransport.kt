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
import org.apache.thrift.TConfiguration
import org.apache.thrift.runCompletable
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousByteChannel

/**
 * This is the most commonly used base transport. It takes an InputStream or
 * an OutputStream or both and uses it/them to perform transport operations.
 * This allows for compatibility with all the nice constructs Java already
 * has to provide a variety of types of streams.
 *
 */
open class TIOStreamTransport : TEndpointTransport {
    /** Underlying stream  */
    protected var stream: AsynchronousByteChannel? = null

    /** Read/write timeout of the channel **/
    var timeout: Int = 0

    /**
     * Subclasses can invoke the default constructor and then assign the input
     * streams in the open method.
     */
    protected constructor(config: TConfiguration) : super(config)

    /**
     * Subclasses can invoke the default constructor and then assign the input
     * streams in the open method.
     */
    protected constructor() : super(TConfiguration())

    /**
     * Input stream constructor, constructs an input only transport.
     *
     * @param config
     * @param stream Stream to read from
     */
    constructor(config: TConfiguration, stream: AsynchronousByteChannel) : super(config) {
        this.stream = stream
    }

    /**
     * Input stream constructor, constructs an input only transport.
     *
     * @param stream Stream to read from
     */
    constructor(stream: AsynchronousByteChannel) : super(TConfiguration()) {
        this.stream = stream
    }

    /**
     *
     * @return false after close is called.
     */
    override val isOpen: Boolean
        get() = stream?.isOpen == true

    /**
     * The streams must already be open. This method does nothing.
     */
    @Throws(TTransportException::class)
    override suspend fun open() {
    }

    /**
     * Closes both the input and output streams.
     */
    override suspend fun close() = withContext(Dispatchers.IO) {
        try {
            try {
                stream?.close()
                return@withContext
            } catch (iox: IOException) {
                LOGGER.warn("Error closing input stream.", iox)
            }
        } finally {
            stream = null
        }
    }

    /**
     * Reads from the underlying input stream if not null.
     */
    @Throws(TTransportException::class)
    override suspend fun read(buf: ByteArray, off: Int, len: Int): Int {
        if (stream == null) {
            throw TTransportException(TTransportException.NOT_OPEN, "Cannot read from null inputStream")
        }
        val bytesRead: Int = try {
            runCompletable(timeout) {
                stream!!.read(ByteBuffer.wrap(buf, off, len), null, it)
            }
        } catch (iox: IOException) {
            throw TTransportException(TTransportException.UNKNOWN, iox)
        }
        if (bytesRead < 0) {
            throw TTransportException(TTransportException.END_OF_FILE, "Socket is closed by peer.")
        }
        return bytesRead
    }

    /**
     * Writes to the underlying output stream if not null.
     */
    @Throws(TTransportException::class)
    override suspend fun write(buf: ByteArray, off: Int, len: Int) {
        if (stream == null) {
            throw TTransportException(TTransportException.NOT_OPEN, "Cannot write to null outputStream")
        }
        try {
            runCompletable<Int>(timeout) {
                stream!!.write(ByteBuffer.wrap(buf, off, len), null, it)
            }
        } catch (iox: IOException) {
            throw TTransportException(TTransportException.UNKNOWN, iox)
        }
    }

    /**
     * Flushes the underlying output stream if not null.
     */
    @Throws(TTransportException::class)
    override suspend fun flush() = withContext(Dispatchers.IO) {
        if (stream == null) {
            throw TTransportException(TTransportException.NOT_OPEN, "Cannot flush null outputStream")
        }
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(TIOStreamTransport::class.java.name)
    }
}
