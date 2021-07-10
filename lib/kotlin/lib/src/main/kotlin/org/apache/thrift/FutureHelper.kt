package org.apache.thrift

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import java.nio.channels.CompletionHandler
import java.util.concurrent.CancellationException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeoutException

suspend fun <T> runCompletable(timeout: Int, invoke: (handler: CompletionHandler<T, Void?>) -> Unit): T =
    coroutineScope {
        val future = CompletableFuture<T>()
        invoke(object : CompletionHandler<T, Void?> {
            override fun completed(result: T, attachment: Void?) {
                future.complete(result)
            }

            override fun failed(exc: Throwable?, attachment: Void?) {
                future.completeExceptionally(exc)
            }
        })
        var timeoutJob: Job? = null
        if (timeout > 0) {
            timeoutJob = launch(Dispatchers.IO) {
                delay(timeout.toLong())
                future.cancel(true)
            }
        }
        try {
            val result = future.await()
            timeoutJob?.cancelAndJoin()
            return@coroutineScope result
        } catch (ex: CancellationException) {
            throw TimeoutException("Completable timed out after: ${timeout}ms")
        }
    }
