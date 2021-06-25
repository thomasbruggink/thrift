package com.testing

import com.testing.api.*

fun main() {
    val union = ABCombiner.b(B("foo", "bar"))
    val error = Error().setErrorCode(ErrorCodes.INVALID_CODE).setReason("Crash!")
    println("${error.getErrorCode()} Reason: ${error.getReason()}")
    val childAll = ChildAll().setInnerVariable(1)
    val hello = HelloAll().setVariable1("Hi")
        .setVariable2(2)
        .setVariable3(3L)
        .setChildNode(childAll)
        .setCombiner(union)
    println("${hello.getVariable1()} ${hello.getVariable2()} ${hello.getChildNode()?.getInnerVariable()}")

    when (hello.getCombiner()?.getFieldValue()) {
        ABCombiner.Fields.A -> {
            val a = hello.getCombiner()?.getA()
            println("Type: a, ${a?.getP1()}")
        }
        ABCombiner.Fields.B -> {
            val b = hello.getCombiner()?.getB()
            println("Type: b, ${b?.getP3()}")
        }
    }
}
