package com.abusix.knsq.protocol

import kotlin.test.Test
import kotlin.test.assertEquals

class ResponseTest {
    @Test
    fun testSize() {
        val str = "bla"
        val response = Response(str)
        assertEquals(str, response.msg)
        assertEquals(str.length + 4, response.size)
    }
}