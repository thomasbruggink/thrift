package org.apache.thrift

import java.io.Serializable

/**
 * Generic base interface for generated Thrift objects.
 *
 */
interface TBase<T : TBase<T, F>?, F : TFieldIdEnum?> : Comparable<T>, TSerializable, Serializable {
    /**
     * Get the F instance that corresponds to fieldId.
     */
    fun fieldForId(fieldId: Int): F?

    /**
     * Check if a field is currently set or unset.
     *
     * @param field
     */
    fun isSet(field: F): Boolean

    /**
     * Get a field's value by field variable. Primitive types will be wrapped in
     * the appropriate "boxed" types.
     *
     * @param field
     */
    fun getFieldValue(field: F): Any?

    /**
     * Set a field's value by field variable. Primitive types must be "boxed" in
     * the appropriate object wrapper type.
     *
     * @param field
     */
    fun setFieldValue(field: F, value: Any?)
    fun deepCopy(): T

    /**
     * Return to the state of having just been initialized, as though you had just
     * called the default constructor.
     */
    fun clear()
}
