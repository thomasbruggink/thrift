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
package org.apache.thrift.protocol

import org.apache.thrift.TException

/**
 * Protocol exceptions.
 *
 */
class TProtocolException : TException {
    var type = UNKNOWN
        protected set

    constructor() : super() {}
    constructor(type: Int) : super() {
        this.type = type
    }

    constructor(type: Int, message: String?) : super(message) {
        this.type = type
    }

    constructor(message: String?) : super(message) {}
    constructor(type: Int, cause: Throwable?) : super(cause) {
        this.type = type
    }

    constructor(cause: Throwable?) : super(cause) {}
    constructor(message: String?, cause: Throwable?) : super(message, cause) {}
    constructor(type: Int, message: String?, cause: Throwable?) : super(message, cause) {
        this.type = type
    }

    companion object {
        private const val serialVersionUID = 1L
        const val UNKNOWN = 0
        const val INVALID_DATA = 1
        const val NEGATIVE_SIZE = 2
        const val SIZE_LIMIT = 3
        const val BAD_VERSION = 4
        const val NOT_IMPLEMENTED = 5
        const val DEPTH_LIMIT = 6
    }
}
