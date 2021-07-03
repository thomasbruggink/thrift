package org.apache.thrift

import java.io.IOException

interface AsyncClosable {
    @Throws(IOException::class)
    suspend fun close()
}
