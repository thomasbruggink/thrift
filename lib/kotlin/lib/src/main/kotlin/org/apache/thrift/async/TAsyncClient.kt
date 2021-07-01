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

import org.apache.thrift.protocol.TProtocolFactory
import org.apache.thrift.transport.TNonblockingTransport

abstract class TAsyncClient(
    val protocolFactory: TProtocolFactory?,
    manager: TAsyncClientManager?,
    transport: TNonblockingTransport,
    timeout: Long
) {
    protected val ___transport: TNonblockingTransport
    protected val ___manager: TAsyncClientManager?
    protected var ___currentMethod: TAsyncMethodCall? = null

    /**
     * Get the client's error - returns null if no error
     * @return Get the client's error.
     *
     * returns null if no error
     */
    var error: Exception? = null
        private set
    var timeout: Long

    constructor(
        protocolFactory: TProtocolFactory?,
        manager: TAsyncClientManager?,
        transport: TNonblockingTransport
    ) : this(protocolFactory, manager, transport, 0) {
    }

    fun hasTimeout(): Boolean {
        return timeout > 0
    }

    /**
     * Is the client in an error state?
     * @return If client in an error state?
     */
    fun hasError(): Boolean {
        return error != null
    }

    protected fun checkReady() {
        // Ensure we are not currently executing a method
        check(___currentMethod == null) {
            "Client is currently executing another method: " + ___currentMethod.getClass().getName()
        }

        // Ensure we're not in an error state
        if (error != null) {
            throw IllegalStateException("Client has an error!", error)
        }
    }

    /**
     * Called by delegate method when finished
     */
    fun onComplete() {
        ___currentMethod = null
    }

    /**
     * Called by delegate method on error
     */
    fun onError(exception: Exception?) {
        ___transport.close()
        ___currentMethod = null
        error = exception
    }

    init {
        ___manager = manager
        ___transport = transport
        this.timeout = timeout
    }
}
