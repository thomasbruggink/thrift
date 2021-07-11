package org.apache.thrift.server

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import org.apache.thrift.TException
import org.apache.thrift.protocol.TProtocol
import org.apache.thrift.transport.TTransport
import org.apache.thrift.transport.TTransportException
import org.slf4j.LoggerFactory

class TDispatcherServer(
    args: AbstractServerArgs<*>,
    private val dispatcher: CoroutineDispatcher
) : TServer(args) {
    override suspend fun serve() = coroutineScope {
        try {
            serverTransport_.listen()
        } catch (ttx: TTransportException) {
            LOGGER.error("Error occurred during listening.", ttx)
            return@coroutineScope
        }

        // Run the preServe event
        eventHandler?.preServe()
        serving = true
        while (!shouldStop) {
            try {
                val client = serverTransport_.accept() ?: continue
                launch(dispatcher) {
                    handleClient(client)
                }
            } catch (ttx: TTransportException) {
                // Client died, just move on
                LOGGER.debug("Client Transportation Exception", ttx)
            } catch (tx: TException) {
                if (!shouldStop) {
                    LOGGER.error("Thrift error occurred during processing of message.", tx)
                }
            } catch (x: Exception) {
                if (!shouldStop) {
                    LOGGER.error("Error occurred during processing of message.", x)
                }
            }
        }
        serving = false
    }

    private suspend fun handleClient(client: TTransport) {
        var inputProtocol: TProtocol? = null
        var outputProtocol: TProtocol? = null
        var connectionContext: ServerContext? = null
        var inputTransport: TTransport? = null
        var outputTransport: TTransport? = null
        try {
            val processor = processorFactory_.getProcessor(client)
            inputTransport = inputTransportFactory_.getTransport(client)
            outputTransport = outputTransportFactory_.getTransport(client)
            inputProtocol = inputProtocolFactory_.getProtocol(inputTransport)
            outputProtocol = outputProtocolFactory_.getProtocol(outputTransport)
            connectionContext = eventHandler?.createContext(inputProtocol, outputProtocol)
            while (!shouldStop) {
                eventHandler?.processContext(connectionContext, inputTransport, outputTransport)
                processor.process(inputProtocol, outputProtocol)
            }
        } catch (ttx: TTransportException) {
            // Client died, just move on
            LOGGER.debug("Client Transportation Exception", ttx)
        } catch (tx: TException) {
            if (!shouldStop) {
                LOGGER.error("Thrift error occurred during processing of message.", tx)
            }
        } catch (x: Exception) {
            if (!shouldStop) {
                LOGGER.error("Error occurred during processing of message.", x)
            }
        }
        eventHandler?.deleteContext(connectionContext, inputProtocol, outputProtocol)
        inputTransport?.close()
        outputTransport?.close()
    }

    override fun stop() {
        shouldStop = true
        serverTransport_.interrupt()
    }

    private companion object {
        private val LOGGER = LoggerFactory.getLogger(TDispatcherServer::class.java.name)
    }
}
