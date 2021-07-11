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

import org.apache.thrift.TProcessor
import org.apache.thrift.TProcessorFactory
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.protocol.TProtocolFactory
import org.apache.thrift.transport.TServerTransport
import org.apache.thrift.transport.TTransportFactory

/**
 * Generic interface for a Thrift server.
 */
abstract class TServer protected constructor(args: AbstractServerArgs<*>) {

    class Args(
        serverTransport: TServerTransport,
        processorFactory: TProcessorFactory
    ) : AbstractServerArgs<Args>(serverTransport, processorFactory)

    abstract class AbstractServerArgs<T : AbstractServerArgs<T>>(
        var serverTransport: TServerTransport,
        var processorFactory: TProcessorFactory
    ) {
        var inputTransportFactory = TTransportFactory()
        var outputTransportFactory = TTransportFactory()
        var inputProtocolFactory: TProtocolFactory = TBinaryProtocol.Factory()
        var outputProtocolFactory: TProtocolFactory = TBinaryProtocol.Factory()

        fun processorFactory(factory: TProcessorFactory): T {
            processorFactory = factory
            return this as T
        }

        fun processor(processor: TProcessor): T {
            processorFactory = TProcessorFactory(processor)
            return this as T
        }

        fun transportFactory(factory: TTransportFactory): T {
            inputTransportFactory = factory
            outputTransportFactory = factory
            return this as T
        }

        fun inputTransportFactory(factory: TTransportFactory): T {
            inputTransportFactory = factory
            return this as T
        }

        fun outputTransportFactory(factory: TTransportFactory): T {
            outputTransportFactory = factory
            return this as T
        }

        fun protocolFactory(factory: TProtocolFactory): T {
            inputProtocolFactory = factory
            outputProtocolFactory = factory
            return this as T
        }

        fun inputProtocolFactory(factory: TProtocolFactory): T {
            inputProtocolFactory = factory
            return this as T
        }

        fun outputProtocolFactory(factory: TProtocolFactory): T {
            outputProtocolFactory = factory
            return this as T
        }
    }

    /**
     * Core processor
     */
    protected var processorFactory_: TProcessorFactory

    /**
     * Server transport
     */
    protected var serverTransport_: TServerTransport

    /**
     * Input Transport Factory
     */
    protected var inputTransportFactory_: TTransportFactory

    /**
     * Output Transport Factory
     */
    protected var outputTransportFactory_: TTransportFactory

    /**
     * Input Protocol Factory
     */
    protected var inputProtocolFactory_: TProtocolFactory

    /**
     * Output Protocol Factory
     */
    protected var outputProtocolFactory_: TProtocolFactory

    @Volatile
    var serving = false
        protected set

    protected var eventHandler: TServerEventHandler? = null

    // Flag for stopping the server
    // Please see THRIFT-1795 for the usage of this flag
    @Volatile
    var shouldStop = false

    /**
     * The run method fires up the server and gets things going.
     */
    abstract suspend fun serve()

    /**
     * Stop the server. This is optional on a per-implementation basis. Not
     * all servers are required to be cleanly stoppable.
     */
    open fun stop() {}

    fun setServerEventHandler(eventHandler: TServerEventHandler?) {
        this.eventHandler = eventHandler
    }

    init {
        processorFactory_ = args.processorFactory
        serverTransport_ = args.serverTransport
        inputTransportFactory_ = args.inputTransportFactory
        outputTransportFactory_ = args.outputTransportFactory
        inputProtocolFactory_ = args.inputProtocolFactory
        outputProtocolFactory_ = args.outputProtocolFactory
    }
}
