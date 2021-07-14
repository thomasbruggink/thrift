package org.apache.thrift.transport.client

import java.math.BigInteger
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousByteChannel
import java.nio.channels.CompletionHandler
import java.time.Duration
import java.util.LinkedList
import java.util.Queue
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Flow
import java.util.concurrent.Flow.Subscriber
import java.util.concurrent.Future
import java.util.concurrent.atomic.AtomicLong
import org.apache.thrift.transport.TIOStreamTransport
import org.apache.thrift.transport.TTransportException

class THttpChunkedTransport(
    val uri: URI,
    private val httpClient: HttpClient
) : TIOStreamTransport() {
    private var sending: Boolean = false
    private var request: HttpRequest? = null
    var requestCount = AtomicLong()
    var bufferCompletable: Queue<CompletableFuture<Void>> = LinkedList()
    var writeSubscriber: Subscriber<in ByteBuffer>? = null
    var readSubscriber: Subscriber<in List<ByteBuffer>>? = null
    var readSubscription: Flow.Subscription? = null

    constructor(uri: URI) : this(uri, Duration.ofSeconds(30))

    constructor(uri: URI, timeout: Duration) : this(uri, HttpClient.newBuilder().connectTimeout(timeout).build())

    init {
        stream = HttpChannel()
        request = HttpRequest.newBuilder(uri)
            .header("Content-Type", "application/x-thrift")
            .header("Accept", "application/x-thrift")
            .POST(HttpRequest.BodyPublishers.fromPublisher { sub ->
                writeSubscriber = sub
                sub.onSubscribe(object : Flow.Subscription {
                    override fun request(n: Long) {
                        synchronized(requestCount) {
                            if (bufferCompletable.isNotEmpty()) {
                                bufferCompletable.peek().complete(null)
                                requestCount.addAndGet(n - 1)
                            } else {
                                requestCount.addAndGet(n)
                            }
                        }
                    }

                    override fun cancel() {
                        println("Canceled")
                    }
                })
            }).build()
    }

    override suspend fun open() {
        super.open()
    }

    override suspend fun close() {
        bufferCompletable.clear()
        writeSubscriber = null
        readSubscriber = null
        readSubscription = null
        request = null
    }

    @Throws(TTransportException::class)
    override suspend fun flush() {
        writeSubscriber?.onComplete()
        readSubscription?.request(1)
        bufferCompletable.clear()
        writeSubscriber = null
        sending = false
    }

    override val bytesRemainingInBuffer: Int
        get() = super.bytesRemainingInBuffer

    inner class HttpChannel : AsynchronousByteChannel {
        private var readCompletable: CompletableFuture<List<ByteBuffer>>? = null
        private var readBuffers: Queue<ByteBuffer> = LinkedList()

        init {
            readSubscriber = object : Subscriber<List<ByteBuffer>> {
                override fun onSubscribe(s: Flow.Subscription?) {
                    readSubscription = s
                    readSubscription?.request(1)
                }

                override fun onNext(item: List<ByteBuffer>?) {
                    readCompletable?.complete(item)
                    readSubscription?.request(1)
                }

                override fun onError(throwable: Throwable?) {
                    readCompletable?.completeExceptionally(throwable)
                }

                override fun onComplete() {
                    readCompletable = null
                }
            }
        }

        override fun close() {
            readCompletable = null
            readBuffers.clear()
        }

        override fun isOpen(): Boolean {
            return true
        }

        private fun processRead(dst: ByteBuffer): Int {
            var size = 0
            while (readBuffers.isNotEmpty() && dst.hasRemaining()) {
                val buffer = readBuffers.peek()
                if (dst.remaining() >= buffer.remaining()) {
                    size += buffer.remaining()
                    dst.put(buffer)
                    readBuffers.remove()
                } else {
                    for (i in 0 until dst.remaining()) {
                        dst.put(buffer.get())
                    }
                }
            }
            return size
        }

        override fun <A : Any?> read(dst: ByteBuffer, attachment: A?, handler: CompletionHandler<Int, in A>) {
            val read = processRead(dst)
            if (read > 0) {
                handler.completed(read, attachment)
                return
            }
            readCompletable = CompletableFuture<List<ByteBuffer>>().apply {
                thenAccept {
                    if (it == null) {
                        handler.failed(
                            TTransportException(
                                TTransportException.NOT_OPEN,
                                "Remote end closed the connection."
                            ),
                            attachment
                        )
                        return@thenAccept
                    }
                    for (buffer in it) {
                        readBuffers.offer(buffer)
                    }
                    handler.completed(processRead(dst), attachment)
                }
                exceptionally {
                    handler.failed(it, attachment)
                    throw it
                }
                whenComplete { _, _ ->
                    readCompletable = null
                }
            }
        }

        override fun read(dst: ByteBuffer?): Future<Int> {
            throw NotImplementedError("Not used")
        }

        override fun <A : Any> write(src: ByteBuffer, attachment: A?, handler: CompletionHandler<Int, in A>) {
            if (!sending) {
                sending = true
                requestCount.set(0)
                httpClient.sendAsync(request, HttpResponse.BodyHandlers.fromSubscriber(readSubscriber))
            }
            val remaining = src.remaining()
            synchronized(requestCount) {
                if (requestCount.get() > 0) {
                    println(
                        "Direct {${writeSubscriber}}: ${
                            BigInteger(
                                src.array().take(remaining).toByteArray()
                            ).toString(16)
                        }"
                    )
                    writeSubscriber?.onNext(src)
                    requestCount.decrementAndGet()
                    handler.completed(remaining, attachment)
                } else {
                    bufferCompletable.offer(CompletableFuture<Void>().apply {
                        thenAccept {
                            println(
                                "Callback {${writeSubscriber}}: ${
                                    BigInteger(
                                        src.array().take(remaining).toByteArray()
                                    ).toString(16)
                                }"
                            )
                            writeSubscriber?.onNext(src)
                            handler.completed(remaining, attachment)
                        }
                        exceptionally {
                            handler.failed(it, attachment)
                            throw it
                        }
                        whenComplete { _, _ ->
                            bufferCompletable.remove()
                        }
                    })
                }
            }
        }

        override fun write(src: ByteBuffer?): Future<Int> {
            throw NotImplementedError("Not used")
        }
    }
}
