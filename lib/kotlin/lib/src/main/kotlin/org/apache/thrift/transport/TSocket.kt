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
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.apache.thrift.TConfiguration
import org.apache.thrift.runCompletable
import org.slf4j.LoggerFactory
import java.io.IOException
import java.net.InetSocketAddress
import java.net.SocketException
import java.net.StandardSocketOptions
import java.nio.channels.AsynchronousSocketChannel

/**
 * Socket implementation of the TTransport interface. To be commented soon!
 *
 */
class TSocket : TIOStreamTransport {
    /**
     * Socket object
     */
    lateinit var socket: AsynchronousSocketChannel
        private set

    /**
     * Remote host
     */
    private var host: String? = null

    /**
     * Remote port
     */
    private var port = 0

    /**
     * Socket timeout - read timeout on the socket
     */
    var socketTimeout
        get() = super.timeout
        set(value) {
            super.timeout = value
        }

    /**
     * Connection timeout
     */
    var connectTimeout = 0

    /**
     * Constructor that takes an already created socket.
     *
     * @param socket Already created socket object
     * @throws TTransportException if there is an error setting up the streams
     */
    constructor(socket: AsynchronousSocketChannel) : super(TConfiguration()) {
        this.socket = socket
        try {
            this.socket.setOption(StandardSocketOptions.SO_LINGER, 0)
            this.socket.setOption(StandardSocketOptions.TCP_NODELAY, true)
            this.socket.setOption(StandardSocketOptions.SO_KEEPALIVE, true)
        } catch (sx: SocketException) {
            LOGGER.warn("Could not configure socket.", sx)
        }
        if (isOpen) {
            try {
                stream = this.socket
            } catch (iox: IOException) {
                runBlocking { close() }
                throw TTransportException(TTransportException.NOT_OPEN, iox)
            }
        }
    }

    /**
     * Creates a new unconnected socket that will connect to the given host
     * on the given port.
     *
     * @param host Remote host
     * @param port Remote port
     */
    constructor(host: String?, port: Int) : this(TConfiguration(), host, port, 0)

    /**
     * Creates a new unconnected socket that will connect to the given host
     * on the given port.
     *
     * @param config  check config
     * @param host    Remote host
     * @param port    Remote port
     * @param timeout Socket timeout and connection timeout
     */
    constructor(config: TConfiguration, host: String?, port: Int, timeout: Int = 0) : this(
        config,
        host,
        port,
        timeout,
        timeout
    )

    /**
     * Creates a new unconnected socket that will connect to the given host
     * on the given port.
     *
     * @param config  check config
     * @param host Remote host
     * @param port Remote port
     */
    constructor(config: TConfiguration, host: String?, port: Int) : this(
        config,
        host,
        port,
        0
    )

    /**
     * Creates a new unconnected socket that will connect to the given host
     * on the given port, with a specific connection timeout and a
     * specific socket timeout.
     *
     * @param config          check config
     * @param host            Remote host
     * @param port            Remote port
     * @param socketTimeout   Socket timeout
     * @param connectTimeout  Connection timeout
     */
    constructor(config: TConfiguration, host: String?, port: Int, socketTimeout: Int, connectTimeout: Int) : super(
        config
    ) {
        this.host = host
        this.port = port
        this.socketTimeout = socketTimeout
        this.connectTimeout = connectTimeout
        initSocket()
    }

    /**
     * Initializes the socket object
     */
    private fun initSocket() {
        socket = AsynchronousSocketChannel.open()
        try {
//            this.socket.setOption(StandardSocketOptions.SO_LINGER, 0)
            this.socket.setOption(StandardSocketOptions.TCP_NODELAY, true)
            this.socket.setOption(StandardSocketOptions.SO_KEEPALIVE, true)
        } catch (sx: SocketException) {
            LOGGER.error("Could not configure socket.", sx)
        }
    }

    /**
     * Checks whether the socket is connected.
     */
    override val isOpen: Boolean
        get() = socket.isOpen

    /**
     * Connects the socket, creating a new socket object if necessary.
     */
    @Throws(TTransportException::class)
    override suspend fun open() {
        if (host == null || host!!.isEmpty()) {
            throw TTransportException(TTransportException.NOT_OPEN, "Cannot open null host.")
        }
        if (port <= 0 || port > 65535) {
            throw TTransportException(TTransportException.NOT_OPEN, "Invalid port $port")
        }
        try {
            runCompletable<Void?>(connectTimeout) {
                socket.connect(InetSocketAddress(host, port), null, it)
            }
            stream = socket
        } catch (iox: IOException) {
            close()
            throw TTransportException(TTransportException.NOT_OPEN, iox)
        }
    }

    /**
     * Closes the socket.
     */
    override suspend fun close() = withContext(Dispatchers.IO) {
        // Close the underlying streams
        super.close()

        // Close the socket
        try {
            socket.close()
        } catch (iox: IOException) {
            LOGGER.warn("Could not close socket.", iox)
        }
    }

    private companion object {
        private val LOGGER = LoggerFactory.getLogger(TSocket::class.java.name)
    }
}
