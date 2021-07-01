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
import org.slf4j.LoggerFactory
import java.io.IOException
import java.io.Serializable
import java.nio.channels.ClosedSelectorException
import java.nio.channels.Selector
import java.nio.channels.spi.SelectorProvider
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.TimeoutException

/**
 * Contains selector thread which transitions method call objects
 */
class TAsyncClientManager {
    private val selectThread: SelectThread
    private val pendingCalls: ConcurrentLinkedQueue<TAsyncMethodCall> = ConcurrentLinkedQueue<TAsyncMethodCall>()
    @Throws(TException::class)
    fun call(method: TAsyncMethodCall) {
        if (!isRunning) {
            throw TException("SelectThread is not running")
        }
        method.prepareMethodCall()
        pendingCalls.add(method)
        selectThread.selector.wakeup()
    }

    fun stop() {
        selectThread.finish()
    }

    val isRunning: Boolean
        get() = selectThread.isAlive

    private inner class SelectThread : Thread() {
        val selector: Selector

        @Volatile
        private var running: Boolean
        private val timeoutWatchSet: TreeSet<TAsyncMethodCall> =
            TreeSet<TAsyncMethodCall>(TAsyncMethodCallTimeoutComparator())

        fun finish() {
            running = false
            selector.wakeup()
        }

        override fun run() {
            while (running) {
                try {
                    try {
                        if (timeoutWatchSet.size == 0) {
                            // No timeouts, so select indefinitely
                            selector.select()
                        } else {
                            // We have a timeout pending, so calculate the time until then and select appropriately
                            val nextTimeout: Long = timeoutWatchSet.first().getTimeoutTimestamp()
                            val selectTime = nextTimeout - System.currentTimeMillis()
                            if (selectTime > 0) {
                                // Next timeout is in the future, select and wake up then
                                selector.select(selectTime)
                            } else {
                                // Next timeout is now or in past, select immediately so we can time out
                                selector.selectNow()
                            }
                        }
                    } catch (e: IOException) {
                        LOGGER.error("Caught IOException in TAsyncClientManager!", e)
                    }
                    transitionMethods()
                    timeoutMethods()
                    startPendingMethods()
                } catch (exception: Exception) {
                    LOGGER.error("Ignoring uncaught exception in SelectThread", exception)
                }
            }
            try {
                selector.close()
            } catch (ex: IOException) {
                LOGGER.warn("Could not close selector. This may result in leaked resources!", ex)
            }
        }

        // Transition methods for ready keys
        private fun transitionMethods() {
            try {
                val keys = selector.selectedKeys().iterator()
                while (keys.hasNext()) {
                    val key = keys.next()
                    keys.remove()
                    if (!key.isValid) {
                        // this can happen if the method call experienced an error and the
                        // key was cancelled. can also happen if we timeout a method, which
                        // results in a channel close.
                        // just skip
                        continue
                    }
                    val methodCall: TAsyncMethodCall = key.attachment() as TAsyncMethodCall
                    methodCall.transition(key)

                    // If done or error occurred, remove from timeout watch set
                    if (methodCall.isFinished() || methodCall.getClient().hasError()) {
                        timeoutWatchSet.remove(methodCall)
                    }
                }
            } catch (e: ClosedSelectorException) {
                LOGGER.error("Caught ClosedSelectorException in TAsyncClientManager!", e)
            }
        }

        // Timeout any existing method calls
        private fun timeoutMethods() {
            val iterator: MutableIterator<TAsyncMethodCall> = timeoutWatchSet.iterator()
            val currentTime = System.currentTimeMillis()
            while (iterator.hasNext()) {
                val methodCall: TAsyncMethodCall = iterator.next()
                if (currentTime >= methodCall.getTimeoutTimestamp()) {
                    iterator.remove()
                    methodCall.onError(
                        TimeoutException(
                            "Operation " + methodCall.getClass()
                                .toString() + " timed out after " + (currentTime - methodCall.getStartTime()).toString() + " ms."
                        )
                    )
                } else {
                    break
                }
            }
        }

        // Start any new calls
        private fun startPendingMethods() {
            var methodCall: TAsyncMethodCall
            while (pendingCalls.poll().also({ methodCall = it }) != null) {
                // Catch registration errors. method will catch transition errors and cleanup.
                try {
                    methodCall.start(selector)

                    // If timeout specified and first transition went smoothly, add to timeout watch set
                    val client: TAsyncClient = methodCall.getClient()
                    if (client.hasTimeout() && !client.hasError()) {
                        timeoutWatchSet.add(methodCall)
                    }
                } catch (exception: Exception) {
                    LOGGER.warn("Caught exception in TAsyncClientManager!", exception)
                    methodCall.onError(exception)
                }
            }
        }

        init {
            selector = SelectorProvider.provider().openSelector()
            running = true
            name = "TAsyncClientManager#SelectorThread " + this.id

            // We don't want to hold up the JVM when shutting down
            isDaemon = true
        }
    }

    /** Comparator used in TreeSet  */
    private class TAsyncMethodCallTimeoutComparator : Comparator<TAsyncMethodCall>, Serializable {
        override fun compare(left: TAsyncMethodCall, right: TAsyncMethodCall): Int {
            return if (left.getTimeoutTimestamp() === right.getTimeoutTimestamp()) {
                (left.getSequenceId() - right.getSequenceId())
            } else {
                (left.getTimeoutTimestamp() - right.getTimeoutTimestamp())
            }
        }
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(TAsyncClientManager::class.java.name)
    }

    init {
        selectThread = SelectThread()
        selectThread.start()
    }
}
