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

import org.apache.thrift.protocol.TField
import org.apache.thrift.protocol.TProtocol
import org.apache.thrift.protocol.TProtocolUtil.skip
import org.apache.thrift.protocol.TStruct
import org.apache.thrift.protocol.TType

/**
 * Application level exception
 *
 */
class TApplicationException : TException, TSerializable {
    var type = UNKNOWN
        protected set
    private var message_: String? = null

    constructor() : super() {}
    constructor(type: Int) : super() {
        this.type = type
    }

    constructor(type: Int, message: String?) : super(message) {
        this.type = type
    }

    constructor(message: String?) : super(message) {}

    override val message: String
        get() = if (message_ == null) {
            super.message!!
        } else {
            message_!!
        }

    @Throws(TException::class)
    override suspend fun read(iprot: TProtocol) {
        var field: TField
        iprot.readStructBegin()
        var message: String? = null
        var type = UNKNOWN
        while (true) {
            field = iprot.readFieldBegin()
            if (field.type == TType.STOP) {
                break
            }
            when (field.id.toInt()) {
                1 -> if (field.type == TType.STRING) {
                    message = iprot.readString()
                } else {
                    skip(iprot, field.type)
                }
                2 -> if (field.type == TType.I32) {
                    type = iprot.readI32()
                } else {
                    skip(iprot, field.type)
                }
                else -> skip(iprot, field.type)
            }
            iprot.readFieldEnd()
        }
        iprot.readStructEnd()
        this.type = type
        message_ = message
    }

    @Throws(TException::class)
    override suspend fun write(oprot: TProtocol) {
        oprot.writeStructBegin(TAPPLICATION_EXCEPTION_STRUCT)
        oprot.writeFieldBegin(MESSAGE_FIELD)
        oprot.writeString(message)
        oprot.writeFieldEnd()
        oprot.writeFieldBegin(TYPE_FIELD)
        oprot.writeI32(type)
        oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()
    }

    companion object {
        private val TAPPLICATION_EXCEPTION_STRUCT = TStruct("TApplicationException")
        private val MESSAGE_FIELD = TField("message", TType.STRING, 1.toShort())
        private val TYPE_FIELD = TField("type", TType.I32, 2.toShort())
        private const val serialVersionUID = 1L
        const val UNKNOWN = 0
        const val UNKNOWN_METHOD = 1
        const val INVALID_MESSAGE_TYPE = 2
        const val WRONG_METHOD_NAME = 3
        const val BAD_SEQUENCE_ID = 4
        const val MISSING_RESULT = 5
        const val INTERNAL_ERROR = 6
        const val PROTOCOL_ERROR = 7
        const val INVALID_TRANSFORM = 8
        const val INVALID_PROTOCOL = 9
        const val UNSUPPORTED_CLIENT_TYPE = 10

        /**
         * Convenience factory method for constructing a TApplicationException given a TProtocol input
         */
        @Throws(TException::class)
        suspend fun readFrom(iprot: TProtocol): TApplicationException {
            val result = TApplicationException()
            result.read(iprot)
            return result
        }
    }
}
