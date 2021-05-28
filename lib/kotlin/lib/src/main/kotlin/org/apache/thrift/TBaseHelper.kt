/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.thrift

import java.io.Serializable
import java.nio.ByteBuffer
import java.util.*
import kotlin.experimental.or

object TBaseHelper {
    private val comparator: Comparator<*> = NestedStructureComparator()
    fun compareTo(o1: Any?, o2: Any?): Int {
        return if (o1 is Comparable<*> && o2 is Comparable<*>) {
            compareTo(o1 as Comparable<*>?, o2 as Comparable<*>?)
        } else if (o1 is List<*>) {
            compareTo(o1, o2 as List<*>)
        } else if (o1 is Set<*>) {
            compareTo(o1, o2 as Set<*>)
        } else if (o1 is Map<*, *>) {
            compareTo(o1, o2 as Map<*, *>)
        } else if (o1 is ByteArray) {
            compareTo(o1, o2 as ByteArray)
        } else {
            throw IllegalArgumentException("Cannot compare objects of type " + o1?.javaClass)
        }
    }

    fun compareTo(a: Boolean, b: Boolean): Int {
        return java.lang.Boolean.compare(a, b)
    }

    fun compareTo(a: Byte, b: Byte): Int {
        return java.lang.Byte.compare(a, b)
    }

    fun compareTo(a: Short, b: Short): Int {
        return java.lang.Short.compare(a, b)
    }

    fun compareTo(a: Int, b: Int): Int {
        return Integer.compare(a, b)
    }

    fun compareTo(a: Long, b: Long): Int {
        return java.lang.Long.compare(a, b)
    }

    fun compareTo(a: Double, b: Double): Int {
        return java.lang.Double.compare(a, b)
    }

    fun compareTo(a: String, b: String?): Int {
        return a.compareTo(b!!)
    }

    fun compareTo(a: ByteArray, b: ByteArray): Int {
        var compare = compareTo(a.size, b.size)
        if (compare == 0) {
            for (i in a.indices) {
                compare = compareTo(a[i], b[i])
                if (compare != 0) {
                    break
                }
            }
        }
        return compare
    }

    fun compareTo(a: Comparable<Any>, b: Comparable<Any>): Int {
        return a.compareTo(b)
    }

    fun compareTo(a: List<*>, b: List<*>): Int {
        var compare = compareTo(a.size, b.size)
        if (compare == 0) {
            for (i in a.indices) {
                compare = comparator.compare(a[i] as Nothing, b[i] as Nothing)
                if (compare != 0) {
                    break
                }
            }
        }
        return compare
    }

    fun compareTo(a: Set<*>, b: Set<*>): Int {
        var compare = compareTo(a.size, b.size)
        if (compare == 0) {
            val sortedA: ArrayList<*> = ArrayList(a)
            val sortedB: ArrayList<*> = ArrayList(b)
            Collections.sort<Any>(sortedA, comparator as Comparator<in Any>?)
            Collections.sort<Any>(sortedB, comparator)
            val iterA: Iterator<*> = sortedA.iterator()
            val iterB: Iterator<*> = sortedB.iterator()

            // Compare each item.
            while (iterA.hasNext() && iterB.hasNext()) {
                compare = comparator.compare(iterA.next(), iterB.next())
                if (compare != 0) {
                    break
                }
            }
        }
        return compare
    }

    fun compareTo(a: Map<Any, Any>, b: Map<Any, Any>): Int {
        var lastComparison = compareTo(a.size, b.size)
        if (lastComparison != 0) {
            return lastComparison
        }

        // Sort a and b so we can compare them.
        val sortedA = TreeMap<Any, Any>(comparator as Comparator<in Any>)
        sortedA.putAll(a)
        val iterA: Iterator<Map.Entry<*, *>> = sortedA.entries.iterator()
        val sortedB: SortedMap<Any, Any> = TreeMap<Any, Any>(comparator as Comparator<in Any?>)
        sortedB.putAll(b)
        val iterB: Iterator<Map.Entry<*, *>> = sortedB.entries.iterator()

        // Compare each item.
        while (iterA.hasNext() && iterB.hasNext()) {
            val (key, value) = iterA.next()
            val (key1, value1) = iterB.next()
            lastComparison = compareTo(key, key1)
            if (lastComparison != 0) {
                return lastComparison
            }
            lastComparison = compareTo(value, value1)
            if (lastComparison != 0) {
                return lastComparison
            }
        }
        return 0
    }

    fun toString(bbs: Collection<ByteBuffer>, sb: StringBuilder) {
        val it = bbs.iterator()
        if (!it.hasNext()) {
            sb.append("[]")
        } else {
            sb.append("[")
            while (true) {
                val bb = it.next()
                toString(bb, sb)
                if (!it.hasNext()) {
                    sb.append("]")
                    return
                } else {
                    sb.append(", ")
                }
            }
        }
    }

    fun toString(bb: ByteBuffer, sb: StringBuilder) {
        val buf = bb.array()
        val arrayOffset = bb.arrayOffset()
        val offset = arrayOffset + bb.position()
        val origLimit = arrayOffset + bb.limit()
        val limit = if (origLimit - offset > 128) offset + 128 else origLimit
        for (i in offset until limit) {
            if (i > offset) {
                sb.append(" ")
            }
            sb.append(paddedByteString(buf[i]))
        }
        if (origLimit != limit) {
            sb.append("...")
        }
    }

    fun paddedByteString(b: Byte): String {
        val extended: Int = (b or 0x100 and 0x1ff).toInt()
        return Integer.toHexString(extended).toUpperCase().substring(1)
    }

    fun byteBufferToByteArray(byteBuffer: ByteBuffer): ByteArray {
        if (wrapsFullArray(byteBuffer)) {
            return byteBuffer.array()
        }
        val target = ByteArray(byteBuffer.remaining())
        byteBufferToByteArray(byteBuffer, target, 0)
        return target
    }

    fun wrapsFullArray(byteBuffer: ByteBuffer): Boolean {
        return (byteBuffer.hasArray()
                && byteBuffer.position() == 0 && byteBuffer.arrayOffset() == 0 && byteBuffer.remaining() == byteBuffer.capacity())
    }

    fun byteBufferToByteArray(byteBuffer: ByteBuffer, target: ByteArray?, offset: Int): Int {
        val remaining = byteBuffer.remaining()
        System.arraycopy(
            byteBuffer.array(),
            byteBuffer.arrayOffset() + byteBuffer.position(),
            target,
            offset,
            remaining
        )
        return remaining
    }

    fun rightSize(`in`: ByteBuffer?): ByteBuffer? {
        if (`in` == null) {
            return null
        }
        return if (wrapsFullArray(`in`)) {
            `in`
        } else ByteBuffer.wrap(byteBufferToByteArray(`in`))
    }

    fun copyBinary(orig: ByteBuffer?): ByteBuffer? {
        if (orig == null) {
            return null
        }
        val copy = ByteBuffer.wrap(ByteArray(orig.remaining()))
        if (orig.hasArray()) {
            System.arraycopy(orig.array(), orig.arrayOffset() + orig.position(), copy.array(), 0, orig.remaining())
        } else {
            orig.slice()[copy.array()]
        }
        return copy
    }

    fun copyBinary(orig: ByteArray?): ByteArray? {
        return if (orig == null) null else Arrays.copyOf(orig, orig.size)
    }

    fun hashCode(value: Long): Int {
        return java.lang.Long.hashCode(value)
    }

    fun hashCode(value: Double): Int {
        return java.lang.Double.hashCode(value)
    }

    /**
     * Comparator to compare items inside a structure (e.g. a list, set, or map).
     */
    private class NestedStructureComparator : Comparator<Any?>, Serializable {
        override fun compare(oA: Any?, oB: Any?): Int {
            return if (oA == null && oB == null) {
                0
            } else if (oA == null) {
                -1
            } else if (oB == null) {
                1
            } else if (oA is List<*>) {
                compareTo(oA, oB as List<*>)
            } else if (oA is Set<*>) {
                compareTo(oA, oB as Set<*>)
            } else if (oA is Map<*, *>) {
                compareTo(oA, oB as Map<*, *>)
            } else if (oA is ByteArray) {
                compareTo(oA, oB as ByteArray)
            } else {
                compareTo(oA as Comparable<*>, oB as Comparable<*>?)
            }
        }
    }
}
