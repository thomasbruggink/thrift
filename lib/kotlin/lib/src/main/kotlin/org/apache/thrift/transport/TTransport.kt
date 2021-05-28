package org.apache.thrift.transport

import org.apache.thrift.TConfiguration
import java.io.Closeable
import java.nio.ByteBuffer

/**
 * Generic class that encapsulates the I/O layer. This is basically a thin
 * wrapper around the combined functionality of Java input/output streams.
 *
 */
abstract class TTransport() : Closeable {
    /**
     * Queries whether the transport is open.
     *
     * @return True if the transport is open.
     */
    abstract val isOpen: Boolean

    /**
     * Is there more data to be read?
     *
     * @return True if the remote side is still alive and feeding us
     */
    fun peek(): Boolean {
        return isOpen
    }

    /**
     * Opens the transport for reading/writing.
     *
     * @throws TTransportException if the transport could not be opened
     */
    @Throws(TTransportException::class)
    abstract fun open()

    /**
     * Closes the transport.
     */
    abstract override fun close()

    /**
     * Reads a sequence of bytes from this channel into the given buffer. An
     * attempt is made to read up to the number of bytes remaining in the buffer,
     * that is, dst.remaining(), at the moment this method is invoked. Upon return
     * the buffer's position will move forward the number of bytes read; its limit
     * will not have changed. Subclasses are encouraged to provide a more
     * efficient implementation of this method.
     *
     * @param dst The buffer into which bytes are to be transferred
     * @return The number of bytes read, possibly zero, or -1 if the channel has
     * reached end-of-stream
     * @throws TTransportException if there was an error reading data
     */
    @Throws(TTransportException::class)
    fun read(dst: ByteBuffer): Int {
        val arr = ByteArray(dst.remaining())
        val n = read(arr, 0, arr.size)
        dst.put(arr, 0, n)
        return n
    }

    /**
     * Reads up to len bytes into buffer buf, starting at offset off.
     *
     * @param buf Array to read into
     * @param off Index to start reading at
     * @param len Maximum number of bytes to read
     * @return The number of bytes actually read
     * @throws TTransportException if there was an error reading data
     */
    @Throws(TTransportException::class)
    abstract fun read(buf: ByteArray?, off: Int, len: Int): Int

    /**
     * Guarantees that all of len bytes are actually read off the transport.
     *
     * @param buf Array to read into
     * @param off Index to start reading at
     * @param len Maximum number of bytes to read
     * @return The number of bytes actually read, which must be equal to len
     * @throws TTransportException if there was an error reading data
     */
    @Throws(TTransportException::class)
    fun readAll(buf: ByteArray?, off: Int, len: Int): Int {
        var got = 0
        var ret: Int
        while (got < len) {
            ret = read(buf, off + got, len - got)
            if (ret <= 0) {
                throw TTransportException(
                    "Cannot read. Remote side has closed. Tried to read "
                            + len
                            + " bytes, but only got "
                            + got
                            + " bytes. (This is often indicative of an internal error on the server side. Please check your server logs.)"
                )
            }
            got += ret
        }
        return got
    }

    /**
     * Writes the buffer to the output
     *
     * @param buf The output data buffer
     * @throws TTransportException if an error occurs writing data
     */
    @Throws(TTransportException::class)
    fun write(buf: ByteArray) {
        write(buf, 0, buf.size)
    }

    /**
     * Writes up to len bytes from the buffer.
     *
     * @param buf The output data buffer
     * @param off The offset to start writing from
     * @param len The number of bytes to write
     * @throws TTransportException if there was an error writing data
     */
    @Throws(TTransportException::class)
    abstract fun write(buf: ByteArray?, off: Int, len: Int)

    /**
     * Writes a sequence of bytes to the buffer. An attempt is made to write all
     * remaining bytes in the buffer, that is, src.remaining(), at the moment this
     * method is invoked. Upon return the buffer's position will updated; its limit
     * will not have changed. Subclasses are encouraged to provide a more efficient
     * implementation of this method.
     *
     * @param src The buffer from which bytes are to be retrieved
     * @return The number of bytes written, possibly zero
     * @throws TTransportException if there was an error writing data
     */
    @Throws(TTransportException::class)
    fun write(src: ByteBuffer): Int {
        val arr = ByteArray(src.remaining())
        src.get(arr)
        write(arr, 0, arr.size)
        return arr.size
    }

    /**
     * Flush any pending data out of a transport buffer.
     *
     * @throws TTransportException if there was an error writing out data.
     */
    @Throws(TTransportException::class)
    open fun flush() {
    }

    /**
     * Access the protocol's underlying buffer directly. If this is not a
     * buffered transport, return null.
     * @return protocol's Underlying buffer
     */
    val buffer: ByteArray?
        get() = null

    /**
     * Return the index within the underlying buffer that specifies the next spot
     * that should be read from.
     * @return index within the underlying buffer that specifies the next spot
     * that should be read from
     */
    val bufferPosition: Int
        get() = 0

    /**
     * Get the number of bytes remaining in the underlying buffer. Returns -1 if
     * this is a non-buffered transport.
     * @return the number of bytes remaining in the underlying buffer. <br></br> Returns -1 if
     * this is a non-buffered transport.
     */
    val bytesRemainingInBuffer: Int
        get() = -1

    /**
     * Consume len bytes from the underlying buffer.
     * @param len
     */
    fun consumeBuffer(len: Int) {}
    abstract val configuration: TConfiguration?

    @Throws(TTransportException::class)
    abstract fun updateKnownMessageSize(size: Long)
    @Throws(TTransportException::class)
    abstract fun checkReadBytesAvailable(numBytes: Long)
}
