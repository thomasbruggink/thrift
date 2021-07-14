package org.apache.thrift.transport.client

import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousByteChannel
import java.nio.channels.CompletionHandler
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.concurrent.Future
import kotlinx.coroutines.future.await
import org.apache.thrift.TConfiguration
import org.apache.thrift.transport.TIOStreamTransport
import org.apache.thrift.transport.TTransportException

class THttpTransport(
    val uri: URI,
    private val httpClient: HttpClient
) : TIOStreamTransport() {
    var responseError: String? = null
    var requestArray: ByteArray = ByteArray(TConfiguration.DEFAULT_MAX_FRAME_SIZE)
    var requestSize: Int = 0
    var requestBuffer: ByteBuffer = ByteBuffer.wrap(requestArray)
    var responseArray: ByteArray? = null
    var responseBuffer: ByteBuffer? = null


    constructor(uri: URI) : this(uri, Duration.ofSeconds(30))

    constructor(uri: URI, timeout: Duration) : this(uri, HttpClient.newBuilder().connectTimeout(timeout).build())

    init {
        stream = HttpChannel()
    }

    override suspend fun open() {
        super.open()
    }

    override suspend fun close() {
    }

    @Throws(TTransportException::class)
    override suspend fun flush() {
        responseError = null
        val request = HttpRequest.newBuilder(uri)
            .POST(HttpRequest.BodyPublishers.ofByteArray(requestArray, 0, requestSize))
            .header("Content-Type", "application/x-thrift")
            .header("Accept", "application/x-thrift")
            .build()
        try {
            val response = httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray()).await()
            if (response.statusCode() != 200) {
                responseError = "${response.statusCode()}: ${String(response.body(), StandardCharsets.UTF_8)}"
                return
            }
            responseArray = response.body()
            responseBuffer = ByteBuffer.wrap(responseArray)
        } finally {
            for (i in 0 until requestSize) {
                requestArray[i] = 0
            }
            requestBuffer.rewind()
            requestSize = 0
        }
    }

    override val bytesRemainingInBuffer: Int
        get() = super.bytesRemainingInBuffer

    inner class HttpChannel : AsynchronousByteChannel {
        override fun close() {
        }

        override fun isOpen(): Boolean {
            return true
        }

        override fun <A : Any?> read(dst: ByteBuffer, attachment: A?, handler: CompletionHandler<Int, in A>) {
            if (responseBuffer == null) {
                handler.failed(
                    TTransportException(TTransportException.NOT_OPEN, "ResponseBuffer is not set"),
                    attachment
                )
                return
            }
            if (responseError != null) {
                handler.failed(TTransportException(TTransportException.NOT_OPEN, responseError), attachment)
                return
            }
            val size = dst.remaining()
            for (i in 0 until size) {
                dst.put(responseBuffer!!.get())
            }
            handler.completed(size, attachment)
        }

        override fun read(dst: ByteBuffer?): Future<Int> {
            throw NotImplementedError("Not used")
        }

        override fun <A : Any> write(src: ByteBuffer, attachment: A?, handler: CompletionHandler<Int, in A>) {
            val size = src.remaining()
            requestSize += size
            requestBuffer.put(src)
            handler.completed(size, attachment)
        }

        override fun write(src: ByteBuffer?): Future<Int> {
            throw NotImplementedError("Not used")
        }
    }
}
