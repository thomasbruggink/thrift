package com.testing

import com.testing.api.MyTestService
import com.testing.api.testOneRequest
import kotlinx.coroutines.coroutineScope
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TSocket
import org.apache.thrift.transport.layered.TFramedTransport

suspend fun main() = coroutineScope {
    println("Building transport")
    val transport = TSocket("localhost", 9090)
    transport.open()
    println("Building framedtransport")
    val framedTransport = TFramedTransport(transport)
    println("Building protocol")
    val protocol = TBinaryProtocol(framedTransport)
    println("Building client")
    val client = MyTestService.AsyncClient(protocol)

    println("Building request")
    val request = testOneRequest("kotlin")
    println("Calling service")
    val result = client.testMethod(request)
    println("Result: ${result.getAnswer()}")
}
