package com.testing

import com.testing.api.HelloAll

fun main() {
    val hello = HelloAll().setVariable1("Hi").setVariable2(2)
    println("${hello.getVariable1()} ${hello.getVariable2()}")
}
