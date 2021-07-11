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

import org.apache.thrift.protocol.TProtocol
import org.apache.thrift.transport.TTransport

/**
 * Interface that can handle events from the server core. To
 * use this you should subclass it and implement the methods that you care
 * about. Your subclass can also store local data that you may care about,
 * such as additional "arguments" to these methods (stored in the object
 * instance's state).
 *
 * TODO: It seems this is a custom code entry point created for some resource management purpose in hive.
 * But when looking into hive code, we see that the argments of TProtocol and TTransport are never used.
 * We probably should remove these arguments from all the methods.
 */
interface TServerEventHandler {
    /**
     * Called before the server begins.
     */
    fun preServe()

    /**
     * Called when a new client has connected and is about to being processing.
     */
    fun createContext(
        input: TProtocol?,
        output: TProtocol?
    ): ServerContext?

    /**
     * Called when a client has finished request-handling to delete server
     * context.
     */
    fun deleteContext(
        serverContext: ServerContext?,
        input: TProtocol?,
        output: TProtocol?
    )

    /**
     * Called when a client is about to call the processor.
     */
    fun processContext(
        serverContext: ServerContext?,
        inputTransport: TTransport?, outputTransport: TTransport?
    )
}
