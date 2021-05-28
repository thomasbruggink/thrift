package org.apache.thrift.transport

import org.apache.thrift.TException

/**
 * Transport exceptions.
 *
 */
class TTransportException : TException {
    var type = UNKNOWN
        protected set

    constructor() : super() {}
    constructor(type: Int) : super() {
        this.type = type
    }

    constructor(type: Int, message: String?) : super(message) {
        this.type = type
    }

    constructor(message: String?) : super(message) {}
    constructor(type: Int, cause: Throwable?) : super(cause) {
        this.type = type
    }

    constructor(cause: Throwable?) : super(cause) {}
    constructor(message: String?, cause: Throwable?) : super(message, cause) {}
    constructor(type: Int, message: String?, cause: Throwable?) : super(message, cause) {
        this.type = type
    }

    companion object {
        private const val serialVersionUID = 1L
        const val UNKNOWN = 0
        const val NOT_OPEN = 1
        const val ALREADY_OPEN = 2
        const val TIMED_OUT = 3
        const val END_OF_FILE = 4
        const val CORRUPTED_DATA = 5
    }
}

