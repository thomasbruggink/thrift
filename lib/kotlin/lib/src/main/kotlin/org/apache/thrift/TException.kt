package org.apache.thrift

/**
 * Generic exception class for Thrift.
 *
 */
open class TException : Exception {
    constructor() : super() {}
    constructor(message: String?) : super(message) {}
    constructor(cause: Throwable?) : super(cause) {}
    constructor(message: String?, cause: Throwable?) : super(message, cause) {}

    companion object {
        private const val serialVersionUID = 1L
    }
}

