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
package org.apache.thrift

import kotlinx.coroutines.runBlocking
import org.apache.thrift.protocol.TMessage
import org.apache.thrift.protocol.TMessageType
import org.apache.thrift.protocol.TProtocol

/**
 * A TServiceClient is used to communicate with a TService implementation
 * across protocols and transports.
 */
abstract class TServiceClient(
        /**
         * Get the TProtocol being used as the input (read) protocol.
         * @return the TProtocol being used as the input (read) protocol.
         */
        var inputProtocol: TProtocol,
        /**
         * Get the TProtocol being used as the output (write) protocol.
         * @return the TProtocol being used as the output (write) protocol.
         */
        var outputProtocol: TProtocol
) {
    constructor(protocol: TProtocol) : this(protocol, protocol)

    protected var seqid_ = 0

    @Throws(TException::class)
    protected fun sendBase(methodName: String, args: TBase<*, *>) {
        sendBase(methodName, args, TMessageType.CALL)
    }

    @Throws(TException::class)
    protected fun sendBaseOneway(methodName: String, args: TBase<*, *>) {
        sendBase(methodName, args, TMessageType.ONEWAY)
    }

    @Throws(TException::class)
    private fun sendBase(methodName: String, args: TBase<*, *>, type: Byte) = runBlocking {
        outputProtocol.writeMessageBegin(TMessage(methodName, type, ++seqid_))
        args.write(outputProtocol)
        outputProtocol.writeMessageEnd()
        outputProtocol.transport.flush()
    }

    @Throws(TException::class)
    protected fun receiveBase(result: TBase<*, *>, methodName: String?) = runBlocking {
        val msg = inputProtocol.readMessageBegin()
        if (msg.type == TMessageType.EXCEPTION) {
            val x = TApplicationException()
            x.read(inputProtocol)
            inputProtocol.readMessageEnd()
            throw x
        }
        if (msg.seqid != seqid_) {
            throw TApplicationException(TApplicationException.BAD_SEQUENCE_ID, String.format("%s failed: out of sequence response: expected %d but got %d", methodName, seqid_, msg.seqid))
        }
        result.read(inputProtocol)
        inputProtocol.readMessageEnd()
    }
}
