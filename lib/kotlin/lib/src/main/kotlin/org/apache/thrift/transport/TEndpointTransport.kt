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

abstract class TEndpointTransport(config: TConfiguration) : TTransport() {
    private val maxMessageSize: Long
        get() = configuration.maxMessageSize.toLong()
    private var knownMessageSize: Long = 0
    private var remainingMessageSize: Long = 0
    override val configuration: TConfiguration = config

    /**
     * Resets RemainingMessageSize to the configured maximum
     * @param newSize
     */
    @Throws(TTransportException::class)
    protected fun resetConsumedMessageSize(newSize: Long) {
        // full reset
        if (newSize < 0) {
            knownMessageSize = maxMessageSize
            remainingMessageSize = maxMessageSize
            return
        }

        // update only: message size can shrink, but not grow
        if (newSize > knownMessageSize) throw TTransportException(
            TTransportException.END_OF_FILE,
            "MaxMessageSize reached"
        )
        knownMessageSize = newSize
        remainingMessageSize = newSize
    }

    /**
     * Updates RemainingMessageSize to reflect then known real message size (e.g. framed transport).
     * Will throw if we already consumed too many bytes or if the new size is larger than allowed.
     * @param size
     */
    @Throws(TTransportException::class)
    override fun updateKnownMessageSize(size: Long) {
        val consumed = knownMessageSize - remainingMessageSize
        resetConsumedMessageSize(if (size == 0L) -1 else size)
        countConsumedMessageBytes(consumed)
    }

    /**
     * Throws if there are not enough bytes in the input stream to satisfy a read of numBytes bytes of data
     * @param numBytes
     */
    @Throws(TTransportException::class)
    override fun checkReadBytesAvailable(numBytes: Long) {
        if (remainingMessageSize < numBytes) throw TTransportException(
            TTransportException.END_OF_FILE,
            "MaxMessageSize reached"
        )
    }

    /**
     * Consumes numBytes from the RemainingMessageSize.
     * @param numBytes
     */
    @Throws(TTransportException::class)
    protected fun countConsumedMessageBytes(numBytes: Long) {
        if (remainingMessageSize >= numBytes) {
            remainingMessageSize -= numBytes
        } else {
            remainingMessageSize = 0
            throw TTransportException(TTransportException.END_OF_FILE, "MaxMessageSize reached")
        }
    }

    init {
        resetConsumedMessageSize(-1)
    }
}
