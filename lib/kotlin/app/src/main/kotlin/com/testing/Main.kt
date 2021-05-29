package com.testing

import com.testing.api.ChildAll
import com.testing.api.Error
import com.testing.api.ErrorCodes
import com.testing.api.HelloAll

fun main() {
    val error = Error().setErrorCode(ErrorCodes.INVALID_CODE).setReason("Crash!")
    println("${error.getErrorCode()} Reason: ${error.getReason()}")
    val childAll = ChildAll().setInnerVariable(1)
    val hello = HelloAll().setVariable1("Hi").setVariable2(2).setVariable3(3L).setChildNode(childAll)
    println("${hello.getVariable1()} ${hello.getVariable2()} ${hello.getChildNode()?.getInnerVariable()}")
}
