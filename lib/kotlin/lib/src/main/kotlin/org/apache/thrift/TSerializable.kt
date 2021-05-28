package org.apache.thrift

import org.apache.thrift.protocol.TProtocol

/**
 * Generic base interface for generated Thrift objects.
 *
 */
interface TSerializable {
    /**
     * Reads the TObject from the given input protocol.
     *
     * @param iprot Input protocol
     */
    @Throws(TException::class)
    fun read(iprot: TProtocol)

    /**
     * Writes the objects out to the protocol
     *
     * @param oprot Output protocol
     */
    @Throws(TException::class)
    fun write(oprot: TProtocol)
}

