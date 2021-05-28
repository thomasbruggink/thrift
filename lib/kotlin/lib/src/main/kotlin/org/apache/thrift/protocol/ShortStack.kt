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

import java.util.*

/**
 * ShortStack is a short-specific Stack implementation written for the express
 * purpose of very fast operations on TCompactProtocol's field id stack. This
 * implementation performs at least 10x faster than java.util.Stack.
 */
internal class ShortStack(initialCapacity: Int) {
    private var vector: ShortArray

    /** Always points to the next location  */
    private var top = 0
    fun pop(): Short {
        return vector[--top]
    }

    fun push(pushed: Short) {
        if (vector.size == top) {
            grow()
        }
        vector[top++] = pushed
    }

    private fun grow() {
        vector = Arrays.copyOf(vector, vector.size shl 1)
    }

    fun clear() {
        top = 0
    }

    override fun toString(): String {
        val sb = StringBuilder()
        sb.append("<ShortStack vector:[")
        for (i in vector.indices) {
            val isTop = i == top - 1
            val value = vector[i]
            if (i != 0) {
                sb.append(' ')
            }
            if (isTop) {
                sb.append(">>").append(value.toInt()).append("<<")
            } else {
                sb.append(value.toInt())
            }
        }
        sb.append("]>")
        return sb.toString()
    }

    init {
        vector = ShortArray(initialCapacity)
    }
}
