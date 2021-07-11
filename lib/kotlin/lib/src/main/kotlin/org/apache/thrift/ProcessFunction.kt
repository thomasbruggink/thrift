package org.apache.thrift

import org.apache.thrift.protocol.TMessage
import org.apache.thrift.protocol.TMessageType
import org.apache.thrift.protocol.TProtocol
import org.apache.thrift.protocol.TProtocolException
import org.apache.thrift.transport.TTransportException
import org.slf4j.LoggerFactory

abstract class ProcessFunction<I, T : TBase<*, *>?>(
    private val methodName: String
) {

    @Throws(TException::class)
    suspend fun process(seqid: Int, iprot: TProtocol, oprot: TProtocol, iface: I) {
        val args = emptyArgsInstance
        try {
            args!!.read(iprot)
        } catch (e: TProtocolException) {
            iprot.readMessageEnd()
            val x = TApplicationException(TApplicationException.PROTOCOL_ERROR, e.message)
            oprot.writeMessageBegin(TMessage(methodName, TMessageType.EXCEPTION, seqid))
            x.write(oprot)
            oprot.writeMessageEnd()
            oprot.transport.flush()
            return
        }
        iprot.readMessageEnd()
        var result: TSerializable? = null
        var msgType = TMessageType.REPLY
        try {
            result = getResult(iface, args)
        } catch (ex: TTransportException) {
            LOGGER.error("Transport error while processing $methodName", ex)
            throw ex
        } catch (ex: TApplicationException) {
            LOGGER.error("Internal application error processing $methodName", ex)
            result = ex
            msgType = TMessageType.EXCEPTION
        } catch (ex: Exception) {
            LOGGER.error("Internal error processing $methodName", ex)
            if (rethrowUnhandledExceptions()) throw RuntimeException(ex.message, ex)
            if (!isOneway) {
                result = TApplicationException(
                    TApplicationException.INTERNAL_ERROR,
                    "Internal error processing $methodName"
                )
                msgType = TMessageType.EXCEPTION
            }
        }
        if (!isOneway) {
            oprot.writeMessageBegin(TMessage(methodName, msgType, seqid))
            result!!.write(oprot)
            oprot.writeMessageEnd()
            oprot.transport.flush()
        }
    }

    @Throws(TException::class)
    private suspend fun handleException(seqid: Int, oprot: TProtocol) {
        if (!isOneway) {
            val x = TApplicationException(
                TApplicationException.INTERNAL_ERROR,
                "Internal error processing $methodName"
            )
            oprot.writeMessageBegin(TMessage(methodName, TMessageType.EXCEPTION, seqid))
            x.write(oprot)
            oprot.writeMessageEnd()
            oprot.transport.flush()
        }
    }

    protected open fun rethrowUnhandledExceptions(): Boolean {
        return false
    }

    protected abstract val isOneway: Boolean

    @Throws(TException::class)
    abstract fun getResult(iface: I, args: T): TBase<*, *>?
    abstract val emptyArgsInstance: T

    companion object {
        private val LOGGER = LoggerFactory.getLogger(ProcessFunction::class.java.name)
    }
}
