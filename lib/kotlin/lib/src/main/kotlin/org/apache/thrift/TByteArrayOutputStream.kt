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

import java.io.ByteArrayOutputStream
import java.nio.charset.Charset

/**
 * Class that allows access to the underlying buf without doing deep
 * copies on it.
 *
 */
class TByteArrayOutputStream constructor(
        private val initialSize: Int = 32
) : ByteArrayOutputStream(initialSize) {
    fun get(): ByteArray {
        return buf
    }

    override fun reset() {
        super.reset()
        if (buf.size > initialSize) {
            buf = ByteArray(initialSize)
        }
    }

    fun len(): Int {
        return count
    }

    override fun toString(charset: Charset): String {
        return String(buf, 0, count, charset)
    }
}
