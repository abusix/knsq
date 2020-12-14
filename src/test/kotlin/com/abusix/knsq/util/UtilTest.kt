package com.abusix.knsq.util

import com.abusix.knsq.common.closeQuietly
import com.abusix.knsq.common.isValidNSQTopicOrChannel
import io.mockk.confirmVerified
import io.mockk.mockk
import io.mockk.verify
import org.slf4j.Logger
import java.io.Closeable
import java.io.IOException
import kotlin.test.Test
import kotlin.test.assertFailsWith

class UtilTest {
    @Test
    fun testCloseQuietly() {
        val throwsIOException = Closeable {
            throw IOException()
        }
        val throwsOtherException = Closeable {
            throw Exception()
        }
        val logger: Logger = mockk(relaxed = true)
        throwsIOException.closeQuietly(logger)
        verify { logger.info(any(), any<Throwable>()) }
        confirmVerified(logger)
        assertFailsWith<Exception> { throwsOtherException.closeQuietly(logger) }
        confirmVerified(logger)
    }

    @Test
    fun testIsValidNSQTopicOrChannel() {
        assert("a".isValidNSQTopicOrChannel())
        assert(!"".isValidNSQTopicOrChannel())
        assert(!"@".isValidNSQTopicOrChannel())
        assert(!"a".repeat(65).isValidNSQTopicOrChannel())
        assert("a".repeat(64).isValidNSQTopicOrChannel())
        assert(!("a".repeat(64) + "#ephemeral").isValidNSQTopicOrChannel())
        assert(("a".repeat(54) + "#ephemeral").isValidNSQTopicOrChannel())
        assert(!("a".repeat(55) + "#ephemeral").isValidNSQTopicOrChannel())
    }
}