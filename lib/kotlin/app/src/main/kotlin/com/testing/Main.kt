package com.testing

import com.testing.api.MyTestService
import com.testing.api.testOneRequest
import com.testing.api.testOneResponse
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withContext
import org.apache.thrift.TProcessorFactory
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.server.TServer
import org.apache.thrift.server.TSimpleServer
import org.apache.thrift.transport.TServerSocket
import org.apache.thrift.transport.TSocket
import org.apache.thrift.transport.layered.TFramedTransport

suspend fun client() {
    println("Building transport")
    val transport = TSocket("localhost", 9090)
    transport.open()
    println("Building framedtransport")
    val framedTransport = TFramedTransport(transport)
    println("Building protocol")
    val protocol = TBinaryProtocol(framedTransport)
    println("Building client")
    val client = MyTestService.AsyncClient.Companion.Factory().getClient(protocol)
//    val client = MyTestService.AsyncClient(protocol)

    println("Building request")
    val request = testOneRequest("kotlin")
    println("Calling service")
    val result = client.testMethod(request)
    println("Result: ${result.getAnswer()}")
}

class MyTestServiceHandler : MyTestService.Iface {
    override fun testMethod(req: testOneRequest?): testOneResponse {
        println("Received request from: ${req?.getName()}")
        return testOneResponse("Hi: ${req?.getName()}")
    }
}

class MyTestServiceAsyncHandler : MyTestService.AsyncIface {
    override suspend fun testMethod(req: testOneRequest?): testOneResponse = withContext(Dispatchers.IO) {
        println("Received request from: ${req?.getName()}")
        return@withContext testOneResponse("Hi: ${req?.getName()}")
    }
}

suspend fun syncServer() {
    println("Building handler")
    val handler = MyTestServiceHandler()
    println("Building processor")
    val processor = MyTestService.Processor(handler)
    println("Building server socket")
    val socket = TServerSocket(9090)
    println("Building server")
    val server = TSimpleServer(TServer.Args(socket, TProcessorFactory(processor)))
    println("Serving")
    server.serve()
    println("Stopped")
}

suspend fun asyncServer() {
    println("Building handler")
    val handler = MyTestServiceAsyncHandler()
    println("Building processor")
    val processor = MyTestService.AsyncProcessor(handler)
    println("Building server socket")
    val socket = TServerSocket(9090)
    println("Building server")
    val server = TSimpleServer(TServer.Args(socket, TProcessorFactory(processor)))
    println("Serving")
    server.serve()
    println("Stopped")
}

suspend fun main() = coroutineScope {
//    syncServer()
    asyncServer()
}
