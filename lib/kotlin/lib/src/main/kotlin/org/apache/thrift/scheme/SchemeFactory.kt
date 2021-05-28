package org.apache.thrift.scheme

interface SchemeFactory<S : IScheme<*>> {
    fun getScheme(): S
}

