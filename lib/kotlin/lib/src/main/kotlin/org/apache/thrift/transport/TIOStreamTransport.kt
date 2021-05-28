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

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.apache.thrift.TConfiguration
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream

/**
 * This is the most commonly used base transport. It takes an InputStream or
 * an OutputStream or both and uses it/them to perform transport operations.
 * This allows for compatibility with all the nice constructs Java already
 * has to provide a variety of types of streams.
 *
 */
class TIOStreamTransport : TEndpointTransport {
    /** Underlying inputStream  */
    protected var inputStream_: InputStream? = null

    /** Underlying outputStream  */
    protected var outputStream_: OutputStream? = null

    /**
     * Subclasses can invoke the default constructor and then assign the input
     * streams in the open method.
     */
    protected constructor(config: TConfiguration?) : super(config) {}

    /**
     * Subclasses can invoke the default constructor and then assign the input
     * streams in the open method.
     */
    protected constructor() : super(TConfiguration()) {}

    /**
     * Input stream constructor, constructs an input only transport.
     *
     * @param config
     * @param is Input stream to read from
     */
    constructor(config: TConfiguration?, `is`: InputStream?) : super(config) {
        inputStream_ = `is`
    }

    /**
     * Input stream constructor, constructs an input only transport.
     *
     * @param is Input stream to read from
     */
    constructor(`is`: InputStream?) : super(TConfiguration()) {
        inputStream_ = `is`
    }

    /**
     * Output stream constructor, constructs an output only transport.
     *
     * @param config
     * @param os Output stream to write to
     */
    constructor(config: TConfiguration?, os: OutputStream?) : super(config) {
        outputStream_ = os
    }

    /**
     * Output stream constructor, constructs an output only transport.
     *
     * @param os Output stream to write to
     */
    constructor(os: OutputStream?) : super(TConfiguration()) {
        outputStream_ = os
    }

    /**
     * Two-way stream constructor.
     *
     * @param config
     * @param is Input stream to read from
     * @param os Output stream to read from
     */
    constructor(config: TConfiguration?, `is`: InputStream?, os: OutputStream?) : super(config) {
        inputStream_ = `is`
        outputStream_ = os
    }

    /**
     * Two-way stream constructor.
     *
     * @param is Input stream to read from
     * @param os Output stream to read from
     */
    constructor(`is`: InputStream?, os: OutputStream?) : super(TConfiguration()) {
        inputStream_ = `is`
        outputStream_ = os
    }

    /**
     *
     * @return false after close is called.
     */
    override val isOpen: Boolean
        get() = inputStream_ != null || outputStream_ != null

    /**
     * The streams must already be open. This method does nothing.
     */
    @Throws(TTransportException::class)
    override fun open() {
    }

    /**
     * Closes both the input and output streams.
     */
    override fun close() {
        try {
            if (inputStream_ != null) {
                try {
                    inputStream_!!.close()
                } catch (iox: IOException) {
                    LOGGER.warn("Error closing input stream.", iox)
                }
            }
            if (outputStream_ != null) {
                try {
                    outputStream_!!.close()
                } catch (iox: IOException) {
                    LOGGER.warn("Error closing output stream.", iox)
                }
            }
        } finally {
            inputStream_ = null
            outputStream_ = null
        }
    }

    /**
     * Reads from the underlying input stream if not null.
     */
    @Throws(TTransportException::class)
    override fun read(buf: ByteArray?, off: Int, len: Int): Int {
        if (inputStream_ == null) {
            throw TTransportException(TTransportException.NOT_OPEN, "Cannot read from null inputStream")
        }
        val bytesRead: Int
        bytesRead = try {
            inputStream_!!.read(buf, off, len)
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
    override fun write(buf: ByteArray?, off: Int, len: Int) {
        if (outputStream_ == null) {
            throw TTransportException(TTransportException.NOT_OPEN, "Cannot write to null outputStream")
        }
        try {
            outputStream_!!.write(buf, off, len)
        } catch (iox: IOException) {
            throw TTransportException(TTransportException.UNKNOWN, iox)
        }
    }

    /**
     * Flushes the underlying output stream if not null.
     */
    @Throws(TTransportException::class)
    override fun flush() {
        if (outputStream_ == null) {
            throw TTransportException(TTransportException.NOT_OPEN, "Cannot flush null outputStream")
        }
        try {
            outputStream_!!.flush()
            resetConsumedMessageSize(-1)
        } catch (iox: IOException) {
            throw TTransportException(TTransportException.UNKNOWN, iox)
        }
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(TIOStreamTransport::class.java.name)
    }
}
