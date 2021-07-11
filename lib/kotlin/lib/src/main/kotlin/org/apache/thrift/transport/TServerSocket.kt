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

import org.apache.thrift.runCompletable
import org.slf4j.LoggerFactory
import java.io.IOException
import java.net.InetSocketAddress
import java.net.StandardSocketOptions
import java.nio.channels.AsynchronousServerSocketChannel
import java.nio.channels.AsynchronousSocketChannel

/**
 * Wrapper around ServerSocket for Thrift.
 *
 */
class TServerSocket(args: ServerSocketTransportArgs) :
    TServerTransport() {
    /**
     * Underlying ServerSocket object
     */
    var serverSocket: AsynchronousServerSocketChannel
        private set

    /**
     * Timeout for client sockets from accept
     */
    private var clientTimeout_ = 0

    class ServerSocketTransportArgs(
        val serverSocket: AsynchronousServerSocketChannel
    ) : AbstractServerTransportArgs<ServerSocketTransportArgs>() {
    }

    /**
     * Creates a server socket from underlying socket object
     */
    /**
     * Creates a server socket from underlying socket object
     */
    constructor(serverSocket: AsynchronousServerSocketChannel, clientTimeout: Int = 0) : this(
        ServerSocketTransportArgs(serverSocket).clientTimeout(clientTimeout)
    )

    /**
     * Creates just a port listening server socket
     */
    /**
     * Creates just a port listening server socket
     */
    constructor(port: Int, clientTimeout: Int = 0) : this(InetSocketAddress(port), clientTimeout)

    constructor(bindAddr: InetSocketAddress, clientTimeout: Int = 0) : this(
        ServerSocketTransportArgs(AsynchronousServerSocketChannel.open()).bindAddr(
            bindAddr
        ).clientTimeout(clientTimeout)
    )

    @Throws(TTransportException::class)
    override fun listen() {
        // Empty
    }

    @Throws(TTransportException::class)
    override suspend fun accept(): TSocket {
        val result: AsynchronousSocketChannel = try {
            runCompletable(0) {
                serverSocket.accept(null, it)
            }
        } catch (e: Exception) {
            throw TTransportException(e)
        } ?: throw TTransportException("Blocking server's accept() may not return NULL")
        val socket = TSocket(result)
        socket.timeout = clientTimeout_
        return socket
    }

    override fun close() {
        try {
            serverSocket.close()
        } catch (iox: IOException) {
            LOGGER.warn("Could not close server socket.", iox)
        }
    }

    override fun interrupt() {
        // The thread-safeness of this is dubious, but Java documentation suggests
        // that it is safe to do this from a different thread context
        close()
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(TServerSocket::class.java.name)
    }

    init {
        clientTimeout_ = args.clientTimeout
        serverSocket = args.serverSocket
        try {
            // Make server socket
            serverSocket = AsynchronousServerSocketChannel.open()
            // Prevent 2MSL delay problem on server restarts
            serverSocket.setOption(StandardSocketOptions.SO_REUSEADDR, true)
            // Bind to listening port
            serverSocket.bind(args.bindAddr, args.backlog)
        } catch (ioe: IOException) {
            close()
            throw TTransportException(
                "Could not create ServerSocket on address ${args.bindAddr.toString()}.", ioe
            )
        }
    }
}
