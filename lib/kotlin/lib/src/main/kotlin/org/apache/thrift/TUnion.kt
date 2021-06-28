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

import org.apache.thrift.TBaseHelper.copyBinary
import org.apache.thrift.TBaseHelper.toString
import org.apache.thrift.protocol.TField
import org.apache.thrift.protocol.TProtocol
import org.apache.thrift.protocol.TProtocolException
import org.apache.thrift.protocol.TStruct
import org.apache.thrift.scheme.IScheme
import org.apache.thrift.scheme.SchemeFactory
import org.apache.thrift.scheme.StandardScheme
import org.apache.thrift.scheme.TupleScheme
import java.nio.ByteBuffer

abstract class TUnion<T : TUnion<T, F>, F : TFieldIdEnum> protected constructor() : TBase<T, F> {
    private val schemes: Map<Class<out IScheme<T>>, SchemeFactory<IScheme<T>>>

    protected var uFieldValue: Any? = null
    protected var uSetField: F? = null

    init {
        uSetField = null
        uFieldValue = null
        schemes = mapOf(
            (StandardScheme::class.java to TUnionStandardSchemeFactory<T, F>()) as Pair<Class<out IScheme<T>>, SchemeFactory<IScheme<T>>>,
            (TupleScheme::class.java to TUnionTupleSchemeFactory<T, F>()) as Pair<Class<out IScheme<T>>, SchemeFactory<IScheme<T>>>
        )
    }

    protected fun fromDeepCopy(other: T) {
        if (other.javaClass != this.javaClass) {
            throw ClassCastException()
        }
        uSetField = other.uSetField
        uFieldValue = deepCopyObject(other.uFieldValue)
    }

    private fun deepCopyObject(o: Any?): Any? {
        return when (o) {
            is TBase<*, *> -> {
                o.deepCopy()
            }
            is ByteBuffer -> {
                copyBinary(o)
            }
            is List<*> -> {
                deepCopyList(o)
            }
            is Set<*> -> {
                deepCopySet(o)
            }
            is Map<*, *> -> {
                deepCopyMap(o)
            }
            else -> {
                o
            }
        }
    }

    private fun deepCopyMap(map: Map<*, *>): Map<*, *> {
        val copy = HashMap<Any?, Any?>(map.size)
        for ((key, value) in map) {
            copy[deepCopyObject(key)] = deepCopyObject(value)
        }
        return copy
    }

    private fun deepCopySet(set: Set<*>): Set<*> {
        val copy = HashSet<Any?>(set.size)
        for (o in set) {
            copy.add(deepCopyObject(o))
        }
        return copy
    }

    private fun deepCopyList(list: List<*>): List<*> {
        val copy = ArrayList<Any?>(list.size)
        for (o in list) {
            copy.add(deepCopyObject(o))
        }
        return copy
    }

    fun getSetField(): F? {
        return uSetField
    }

    fun getFieldValue(): Any? {
        return uFieldValue
    }

    override fun getFieldValue(field: F): Any? {
        require(!(field !== uSetField)) { "Cannot get the value of field $field because union's set field is $uSetField" }
        return uFieldValue
    }

    fun getFieldValue(fieldId: Int): Any? {
        return getFieldValue(enumForId(fieldId.toShort()))
    }

    val isSet: Boolean
        get() = uSetField != null

    override fun isSet(field: F): Boolean {
        return uSetField === field
    }

    fun isSet(fieldId: Int): Boolean {
        return isSet(enumForId(fieldId.toShort()))
    }

    @Throws(TException::class)
    override fun read(iprot: TProtocol) {
        schemes[iprot.scheme]!!.getScheme().read(iprot, this as T)
    }

    final override fun setFieldValue(field: F, value: Any?) {
        checkType(field, value)
        uSetField = field
        uFieldValue = value
    }

    fun setFieldValue(fieldId: Int, value: Any?) {
        setFieldValue(enumForId(fieldId.toShort()), value)
    }

    @Throws(TException::class)
    override fun write(oprot: TProtocol) {
        schemes[oprot.scheme]!!.getScheme().write(oprot, this as T)
    }

    /**
     * Implementation should be generated so that we can efficiently type check
     * various values.
     * @param setField
     * @param value
     */
    @Throws(ClassCastException::class)
    protected abstract fun checkType(setField: F, value: Any?)

    /**
     * Implementation should be generated to read the right stuff from the wire
     * based on the field header.
     * @param field
     * @return read Object based on the field header, as specified by the argument.
     */
    @Throws(TException::class)
    protected abstract fun standardSchemeReadValue(iprot: TProtocol, field: TField): Any?

    @Throws(TException::class)
    protected abstract fun standardSchemeWriteValue(oprot: TProtocol)

    @Throws(TException::class)
    protected abstract fun tupleSchemeReadValue(iprot: TProtocol, fieldID: Short): Any?

    @Throws(TException::class)
    protected abstract fun tupleSchemeWriteValue(oprot: TProtocol)
    protected abstract fun getStructDesc(): TStruct?

    protected abstract fun getFieldDesc(setField: F?): TField
    protected abstract fun enumForId(id: Short): F
    override fun toString(): String {
        val sb = StringBuilder()
        sb.append("<")
        sb.append(this.javaClass.simpleName)
        sb.append(" ")
        if (uSetField != null) {
            val v = uFieldValue
            sb.append(getFieldDesc(uSetField!!).name)
            sb.append(":")
            if (v is ByteBuffer) {
                toString((v as ByteBuffer?)!!, sb)
            } else {
                sb.append(v.toString())
            }
        }
        sb.append(">")
        return sb.toString()
    }

    override fun clear() {
        uSetField = null
        uFieldValue = null
    }

    private class TUnionStandardSchemeFactory<T : TUnion<T, F>, F : TFieldIdEnum> :
        SchemeFactory<TUnionStandardScheme<T, F>> {
        override fun getScheme(): TUnionStandardScheme<T, F> {
            return TUnionStandardScheme()
        }
    }

    private class TUnionStandardScheme<T : TUnion<T, F>, F : TFieldIdEnum> : StandardScheme<T>() {
        @Throws(TException::class)
        override fun read(iprot: TProtocol, struct: T) {
            struct.uSetField = null
            struct.uFieldValue = null
            iprot.readStructBegin()
            val field = iprot.readFieldBegin()
            struct.uFieldValue = struct.standardSchemeReadValue(iprot, field)
            if (struct.uFieldValue != null) {
                struct.uSetField = struct.enumForId(field.id)
            }
            iprot.readFieldEnd()
            // this is so that we will eat the stop byte. we could put a check here to
            // make sure that it actually *is* the stop byte, but it's faster to do it
            // this way.
            iprot.readFieldBegin()
            iprot.readStructEnd()
        }

        @Throws(TException::class)
        override fun write(oprot: TProtocol, struct: T) {
            if (struct.uSetField == null || struct.uFieldValue == null) {
                throw TProtocolException("Cannot write a TUnion with no set value!")
            }
            oprot.writeStructBegin(struct.getStructDesc())
            oprot.writeFieldBegin(struct.getFieldDesc(struct.uSetField!!))
            struct.standardSchemeWriteValue(oprot)
            oprot.writeFieldEnd()
            oprot.writeFieldStop()
            oprot.writeStructEnd()
        }
    }

    private class TUnionTupleSchemeFactory<T : TUnion<T, F>, F : TFieldIdEnum> :
        SchemeFactory<TUnionTupleScheme<T, F>> {
        override fun getScheme(): TUnionTupleScheme<T, F> {
            return TUnionTupleScheme()
        }
    }

    private class TUnionTupleScheme<T : TUnion<T, F>, F : TFieldIdEnum> : TupleScheme<T>() {
        @Throws(TException::class)
        override fun read(iprot: TProtocol, struct: T) {
            struct.uSetField = null
            struct.uFieldValue = null
            val fieldID = iprot.readI16()
            struct.uFieldValue = struct.tupleSchemeReadValue(iprot, fieldID)
            if (struct.uFieldValue != null) {
                struct.uSetField = struct.enumForId(fieldID)
            }
        }

        @Throws(TException::class)
        override fun write(oprot: TProtocol, struct: T) {
            if (struct.uSetField == null || struct.uFieldValue == null) {
                throw TProtocolException("Cannot write a TUnion with no set value!")
            }
            oprot.writeI16(struct.uSetField!!.thriftFieldId)
            struct.tupleSchemeWriteValue(oprot)
        }
    }
}
