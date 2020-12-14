package com.abusix.knsq.protocol

import kotlin.test.Test
import kotlin.test.assertEquals

class ErrorTest {
    @Test
    fun testAuthError() {
        val str = "E_AUTH_FAILED"
        val error = Error(str)
        assertEquals(ErrorType.E_AUTH_FAILED, error.errorType)
        assertEquals(str.length + 4, error.size)
    }

    @Test
    fun testRandomString() {
        val str = "some random error string"
        val error = Error(str)
        assertEquals(ErrorType.E_INVALID, error.errorType)
        assertEquals(str.length + 4, error.size)
    }
}