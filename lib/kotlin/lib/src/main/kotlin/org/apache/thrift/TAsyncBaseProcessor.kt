package org.apache.thrift

import org.apache.thrift.protocol.*
import org.apache.thrift.protocol.TProtocolUtil.skip
import java.util.*

abstract class TAsyncBaseProcessor<I> protected constructor(
    private val iface: I,
    private val processFunctionMap: Map<String, AsyncProcessFunction<I, out TBase<*, *>>>
) : TProcessor {
    val processMapView: Map<String, Any>
        get() = Collections.unmodifiableMap(processFunctionMap)

    @Throws(TException::class)
    override suspend fun process(iprot: TProtocol, oprot: TProtocol) {
        val msg = iprot.readMessageBegin()
        val fn: AsyncProcessFunction<I, out TBase<*, *>>? = processFunctionMap[msg.name]
        if (fn == null) {
            skip(iprot, TType.STRUCT)
            iprot.readMessageEnd()
            val x =
                TApplicationException(TApplicationException.UNKNOWN_METHOD, "Invalid method name: '" + msg.name + "'")
            oprot.writeMessageBegin(TMessage(msg.name, TMessageType.EXCEPTION, msg.seqid))
            x.write(oprot)
            oprot.writeMessageEnd()
            oprot.transport.flush()
        } else {
            fn.process(msg.seqid, iprot, oprot, iface)
        }
    }
}
