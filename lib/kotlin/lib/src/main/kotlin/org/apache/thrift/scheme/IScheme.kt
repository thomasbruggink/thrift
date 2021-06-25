package org.apache.thrift.scheme

import org.apache.thrift.TBase
import org.apache.thrift.TException
import org.apache.thrift.protocol.TProtocol


interface IScheme<T : TBase<T, *>> {
    @Throws(TException::class)
    fun read(iprot: TProtocol, struct: T)

    @Throws(TException::class)
    fun write(oprot: TProtocol, struct: T)
}
