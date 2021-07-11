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
package org.apache.thrift.server

import org.apache.thrift.TException
import org.apache.thrift.TProcessor
import org.apache.thrift.protocol.TProtocol
import org.apache.thrift.transport.TTransport
import org.apache.thrift.transport.TTransportException
import org.slf4j.LoggerFactory

/**
 * Simple singlethreaded server for testing.
 */
class TSimpleServer(args: AbstractServerArgs<*>) : TServer(args) {
    override suspend fun serve() {
        try {
            serverTransport_.listen()
        } catch (ttx: TTransportException) {
            LOGGER.error("Error occurred during listening.", ttx)
            return
        }

        // Run the preServe event
        eventHandler?.preServe()
        serving = true
        while (!shouldStop) {
            var client: TTransport?
            val processor: TProcessor?
            var inputTransport: TTransport? = null
            var outputTransport: TTransport? = null
            var inputProtocol: TProtocol? = null
            var outputProtocol: TProtocol? = null
            var connectionContext: ServerContext? = null
            try {
                client = serverTransport_.accept()
                if (client != null) {
                    processor = processorFactory_.getProcessor(client)
                    inputTransport = inputTransportFactory_.getTransport(client)
                    outputTransport = outputTransportFactory_.getTransport(client)
                    inputProtocol = inputProtocolFactory_.getProtocol(inputTransport)
                    outputProtocol = outputProtocolFactory_.getProtocol(outputTransport)
                    connectionContext = eventHandler?.createContext(inputProtocol, outputProtocol)
                    while (true) {
                        eventHandler?.processContext(connectionContext, inputTransport, outputTransport)
                        processor.process(inputProtocol, outputProtocol)
                    }
                }
            } catch (ttx: TTransportException) {
                // Client died, just move on
                LOGGER.debug("Client Transportation Exception", ttx)
            } catch (tx: TException) {
                if (!shouldStop) {
                    LOGGER.error("Thrift error occurred during processing of message.", tx)
                }
            } catch (x: Exception) {
                if (!shouldStop) {
                    LOGGER.error("Error occurred during processing of message.", x)
                }
            }
            eventHandler?.deleteContext(connectionContext, inputProtocol, outputProtocol)
            inputTransport?.close()
            outputTransport?.close()
        }
        serving = false
    }

    override fun stop() {
        shouldStop = true
        serverTransport_.interrupt()
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(TSimpleServer::class.java.name)
    }
}
