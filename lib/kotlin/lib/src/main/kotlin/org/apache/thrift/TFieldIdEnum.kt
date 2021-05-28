package org.apache.thrift

/**
 * Interface for all generated struct Fields objects.
 */
interface TFieldIdEnum {
    /**
     * Get the Thrift field id for the named field.
     */
    val thriftFieldId: Short

    /**
     * Get the field's name, exactly as in the IDL.
     */
    val fieldName: String?
}
