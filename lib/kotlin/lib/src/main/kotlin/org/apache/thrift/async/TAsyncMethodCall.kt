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
package org.apache.thrift.async

import org.apache.thrift.TException
import org.apache.thrift.protocol.TProtocol
import org.apache.thrift.protocol.TProtocolFactory
import org.apache.thrift.transport.TMemoryBuffer
import org.apache.thrift.transport.TNonblockingTransport
import org.apache.thrift.transport.TTransportException
import org.apache.thrift.transport.layered.TFramedTransport
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.SelectionKey
import java.nio.channels.Selector
import java.util.concurrent.atomic.AtomicLong

/**
 * Encapsulates an async method call.
 *
 *
 * Need to generate:
 *
 *  * protected abstract void write_args(TProtocol protocol)
 *  * protected abstract T getResult() throws &lt;Exception_1&gt;, &lt;Exception_2&gt;, ...
 *
 *
 * @param <T> The return type of the encapsulated method call.
</T> */
abstract class TAsyncMethodCall<T> protected constructor(
    client: TAsyncClient,
    protocolFactory: TProtocolFactory,
    protected val transport: TNonblockingTransport,
    callback: AsyncMethodCallback<T>,
    isOneway: Boolean
) {
    enum class State {
        CONNECTING, WRITING_REQUEST_SIZE, WRITING_REQUEST_BODY, READING_RESPONSE_SIZE, READING_RESPONSE_BODY, RESPONSE_READ, ERROR
    }

    /**
     * Next step in the call, initialized by start()
     */
    protected var state: State? = null
        private set
    private val protocolFactory: TProtocolFactory
    val client: TAsyncClient
    private val callback: AsyncMethodCallback<T>
    private val isOneway: Boolean
    protected val sequenceId: Long
    private val timeout: Long
    private var sizeBuffer: ByteBuffer? = null
    private val sizeBufferArray = ByteArray(4)
    protected var frameBuffer: ByteBuffer? = null
        private set
    protected val startTime = System.currentTimeMillis()
    protected val isFinished: Boolean
        protected get() = state == State.RESPONSE_READ

    fun hasTimeout(): Boolean {
        return timeout > 0
    }

    val timeoutTimestamp: Long
        get() = timeout + startTime

    @Throws(TException::class)
    protected abstract fun write_args(protocol: TProtocol?)

    @get:Throws(Exception::class)
    protected abstract val result: T

    /**
     * Initialize buffers.
     * @throws TException if buffer initialization fails
     */
    @Throws(TException::class)
    protected fun prepareMethodCall() {
        val memoryBuffer = TMemoryBuffer(INITIAL_MEMORY_BUFFER_SIZE)
        val protocol = protocolFactory.getProtocol(memoryBuffer)
        write_args(protocol)
        val length: Int = memoryBuffer.length()
        frameBuffer = ByteBuffer.wrap(memoryBuffer.getArray(), 0, length)
        TFramedTransport.encodeFrameSize(length, sizeBufferArray)
        sizeBuffer = ByteBuffer.wrap(sizeBufferArray)
    }

    /**
     * Register with selector and start first state, which could be either connecting or writing.
     * @throws IOException if register or starting fails
     */
    @Throws(IOException::class)
    fun start(sel: Selector?) {
        val key: SelectionKey?
        if (transport.isOpen) {
            state = State.WRITING_REQUEST_SIZE
            key = transport.registerSelector(sel, SelectionKey.OP_WRITE)
        } else {
            state = State.CONNECTING
            key = transport.registerSelector(sel, SelectionKey.OP_CONNECT)

            // non-blocking connect can complete immediately,
            // in which case we should not expect the OP_CONNECT
            if (transport.startConnect()) {
                registerForFirstWrite(key)
            }
        }
        key!!.attach(this)
    }

    @Throws(IOException::class)
    fun registerForFirstWrite(key: SelectionKey?) {
        state = State.WRITING_REQUEST_SIZE
        key!!.interestOps(SelectionKey.OP_WRITE)
    }

    /**
     * Transition to next state, doing whatever work is required. Since this
     * method is only called by the selector thread, we can make changes to our
     * select interests without worrying about concurrency.
     * @param key
     */
    fun transition(key: SelectionKey) {
        // Ensure key is valid
        if (!key.isValid) {
            key.cancel()
            val e: Exception = TTransportException("Selection key not valid!")
            onError(e)
            return
        }

        // Transition function
        try {
            when (state) {
                State.CONNECTING -> doConnecting(key)
                State.WRITING_REQUEST_SIZE -> doWritingRequestSize()
                State.WRITING_REQUEST_BODY -> doWritingRequestBody(key)
                State.READING_RESPONSE_SIZE -> doReadingResponseSize()
                State.READING_RESPONSE_BODY -> doReadingResponseBody(key)
                else -> throw IllegalStateException(
                    "Method call in state " + state
                            + " but selector called transition method. Seems like a bug..."
                )
            }
        } catch (e: Exception) {
            key.cancel()
            key.attach(null)
            onError(e)
        }
    }

    protected fun onError(e: Exception?) {
        client.onError(e)
        callback.onError(e)
        state = State.ERROR
    }

    @Throws(TTransportException::class)
    private fun doReadingResponseBody(key: SelectionKey) {
        if (transport.read((frameBuffer)!!) < 0) {
            throw TTransportException(TTransportException.END_OF_FILE, "Read call frame failed")
        }
        if (frameBuffer!!.remaining() == 0) {
            cleanUpAndFireCallback(key)
        }
    }

    private fun cleanUpAndFireCallback(key: SelectionKey) {
        state = State.RESPONSE_READ
        key.interestOps(0)
        // this ensures that the TAsyncMethod instance doesn't hang around
        key.attach(null)
        try {
            val result = result
            client.onComplete()
            callback.onComplete(result)
        } catch (e: Exception) {
            key.cancel()
            onError(e)
        }
    }

    @Throws(TTransportException::class)
    private fun doReadingResponseSize() {
        if (transport.read((sizeBuffer)!!) < 0) {
            throw TTransportException(TTransportException.END_OF_FILE, "Read call frame size failed")
        }
        if (sizeBuffer!!.remaining() == 0) {
            state = State.READING_RESPONSE_BODY
            frameBuffer = ByteBuffer.allocate(TFramedTransport.decodeFrameSize(sizeBufferArray))
        }
    }

    @Throws(TTransportException::class)
    private fun doWritingRequestBody(key: SelectionKey) {
        if (transport.write((frameBuffer)!!) < 0) {
            throw TTransportException(TTransportException.END_OF_FILE, "Write call frame failed")
        }
        if (frameBuffer!!.remaining() == 0) {
            if (isOneway) {
                cleanUpAndFireCallback(key)
            } else {
                state = State.READING_RESPONSE_SIZE
                sizeBuffer!!.rewind() // Prepare to read incoming frame size
                key.interestOps(SelectionKey.OP_READ)
            }
        }
    }

    @Throws(TTransportException::class)
    private fun doWritingRequestSize() {
        if (transport.write((sizeBuffer)!!) < 0) {
            throw TTransportException(TTransportException.END_OF_FILE, "Write call frame size failed")
        }
        if (sizeBuffer!!.remaining() == 0) {
            state = State.WRITING_REQUEST_BODY
        }
    }

    @Throws(IOException::class)
    private fun doConnecting(key: SelectionKey) {
        if (!key.isConnectable || !transport.finishConnect()) {
            throw IOException("not connectable or finishConnect returned false after we got an OP_CONNECT")
        }
        registerForFirstWrite(key)
    }

    companion object {
        private val INITIAL_MEMORY_BUFFER_SIZE = 128
        private val sequenceIdCounter = AtomicLong(0)
    }

    init {
        this.callback = callback
        this.protocolFactory = protocolFactory
        this.client = client
        this.isOneway = isOneway
        sequenceId = sequenceIdCounter.getAndIncrement()
        timeout = client.timeout
    }
}
