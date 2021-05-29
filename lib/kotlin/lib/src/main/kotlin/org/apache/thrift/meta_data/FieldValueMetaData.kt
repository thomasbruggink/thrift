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
package org.apache.thrift.meta_data

import org.apache.thrift.protocol.TType
import java.io.Serializable

/**
 * FieldValueMetaData and collection of subclasses to store metadata about
 * the value(s) of a field
 */
open class FieldValueMetaData : Serializable {
    val type: Byte
    val isTypedef: Boolean
    val typedefName: String?
    val isBinary: Boolean

    @JvmOverloads
    constructor(type: Byte, binary: Boolean = false) {
        this.type = type
        isTypedef = false
        typedefName = null
        isBinary = binary
    }

    constructor(type: Byte, typedefName: String?) {
        this.type = type
        isTypedef = true
        this.typedefName = typedefName
        isBinary = false
    }

    val isStruct: Boolean
        get() = type == TType.STRUCT
    val isContainer: Boolean
        get() = type == TType.LIST || type == TType.MAP || type == TType.SET
}
