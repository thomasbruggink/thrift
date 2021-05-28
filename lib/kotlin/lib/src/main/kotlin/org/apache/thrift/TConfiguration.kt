package org.apache.thrift

class TConfiguration @JvmOverloads constructor(
    var maxMessageSize: Int = DEFAULT_MAX_MESSAGE_SIZE,
    var maxFrameSize: Int = DEFAULT_MAX_FRAME_SIZE,
    var recursionLimit: Int = DEFAULT_RECURSION_DEPTH
) {

    class Builder internal constructor() {
        private var maxMessageSize: Int
        private var maxFrameSize: Int
        private var recursionLimit: Int
        fun setMaxMessageSize(maxMessageSize: Int): Builder {
            this.maxMessageSize = maxMessageSize
            return this
        }

        fun setMaxFrameSize(maxFrameSize: Int): Builder {
            this.maxFrameSize = maxFrameSize
            return this
        }

        fun setRecursionLimit(recursionLimit: Int): Builder {
            this.recursionLimit = recursionLimit
            return this
        }

        fun build(): TConfiguration {
            return TConfiguration(maxMessageSize, maxFrameSize, recursionLimit)
        }

        init {
            maxFrameSize = DEFAULT_MAX_FRAME_SIZE
            maxMessageSize = DEFAULT_MAX_MESSAGE_SIZE
            recursionLimit = DEFAULT_RECURSION_DEPTH
        }
    }

    companion object {
        const val DEFAULT_MAX_MESSAGE_SIZE = 100 * 1024 * 1024
        const val DEFAULT_MAX_FRAME_SIZE = 16384000 // this value is used consistently across all Thrift libraries
        const val DEFAULT_RECURSION_DEPTH = 64
        val DEFAULT = Builder().build()
        fun custom(): Builder {
            return Builder()
        }
    }
}

