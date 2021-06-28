/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.thrift

/**
 * Implementation of the Option type pattern
 */
abstract class Option<T> {
    /**
     * Whether the Option is defined or not
     * @return
     * true if the Option is defined (of type Some)
     * false if the Option is not defined (of type None)
     */
    abstract fun isDefined(): Boolean

    /**
     * Get the value of the Option (if it is defined)
     * @return the value
     * @throws IllegalStateException if called on a None
     */
    abstract fun get(): T

    /**
     * Get the contained value (if defined) or else return a default value
     * @param other what to return if the value is not defined (a None)
     * @return either the value, or other if the value is not defined
     */
    fun or(other: T): T {
        return if (isDefined()) {
            get()
        } else {
            other
        }
    }

    /**
     * The None type, representing an absent value (instead of "null")
     */
    class None<T> : Option<T>() {
        override fun isDefined(): Boolean {
            return false
        }

        override fun get(): T {
            throw IllegalStateException("Cannot call get() on None")
        }

        override fun toString(): String {
            return "None"
        }
    }

    /**
     * The Some type, representing an existence of some value
     * @param <T> The type of value
    </T> */
    class Some<T>(private val value: T) : Option<T>() {
        override fun isDefined(): Boolean {
            return true
        }

        override fun get(): T {
            return value
        }

        override fun toString(): String {
            return "Some($value)"
        }
    }

    companion object {
        private val NONE: Option<*> = None<Any?>()

        /**
         * Wraps value in an Option type, depending on whether or not value is null
         * @param value
         * @param <T> type of value
         * @return Some(value) if value is not null, None if value is null
        </T> */
        fun <T> fromNullable(value: T?): Option<T> {
            return if (value != null) {
                some(value)
            } else {
                none()
            }
        }

        /**
         * Wrap value in a Some type (NB! value must not be null!)
         * @param value
         * @param <T> type of value
         * @return a new Some(value)
        </T> */
        fun <T> some(value: T): Some<T> {
            return Some(value)
        }

        fun <T> none(): None<T> {
            return NONE as None<T>
        }
    }
}
