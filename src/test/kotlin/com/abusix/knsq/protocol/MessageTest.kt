package com.abusix.knsq.protocol

import com.abusix.knsq.connection.Connection
import com.abusix.knsq.connection.SubConnection
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import java.time.Instant
import java.time.temporal.ChronoUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class MessageTest {

    private val con: SubConnection = mockk(relaxed = true)
    private val ts = Instant.ofEpochSecond(56789000)
    private val msg = Message(ChronoUnit.NANOS.between(Instant.EPOCH, ts), 0, "blabla", "data".toByteArray(), con)

    @Test
    fun testSize() {
        assertEquals(34, msg.size)
    }

    @Test
    fun testTimestamp() {
        assertEquals(ts, msg.timestamp)
    }

    @Test
    fun testFinish() {
        msg.finish()
        assert(msg.isFinished)
        assert(!msg.isRequeued)
        verify(exactly = 1) { con.finish(msg.id) }
        confirmVerified(con)
    }

    @Test
    fun testFinishTwice() {
        msg.finish()
        msg.finish()
        assert(msg.isFinished)
        assert(!msg.isRequeued)
        verify(exactly = 1) { con.finish(msg.id) }
        confirmVerified(con)
    }

    @Test
    fun testFinishAfterRequeue() {
        msg.requeue()
        msg.finish()
        assert(!msg.isFinished)
        assert(msg.isRequeued)
        verify(exactly = 1) { con.requeue(msg.id) }
        confirmVerified(con)
    }

    @Test
    fun testRequeue() {
        msg.requeue()
        assert(!msg.isFinished)
        assert(msg.isRequeued)
        verify(exactly = 1) { con.requeue(msg.id) }
        confirmVerified(con)
    }

    @Test
    fun testRequeueTwice() {
        msg.requeue()
        msg.requeue()
        assert(!msg.isFinished)
        assert(msg.isRequeued)
        verify(exactly = 1) { con.requeue(msg.id) }
        confirmVerified(con)
    }

    @Test
    fun testRequeueAfterFinished() {
        msg.finish()
        msg.requeue()
        assert(msg.isFinished)
        assert(!msg.isRequeued)
        verify(exactly = 1) { con.finish(msg.id) }
        confirmVerified(con)
    }

    @Test
    fun testTouch() {
        msg.touch()
        assert(!msg.isFinished)
        assert(!msg.isRequeued)
        verify(exactly = 1) { con.touch(msg.id) }
        confirmVerified(con)
    }

    @Test
    fun testTouchTwice() {
        msg.touch()
        msg.touch()
        assert(!msg.isFinished)
        assert(!msg.isRequeued)
        verify(exactly = 2) { con.touch(msg.id) }
        confirmVerified(con)
    }

    @Test
    fun testTouchAfterRequeue() {
        msg.requeue()
        msg.touch()
        assert(!msg.isFinished)
        assert(msg.isRequeued)
        verify(exactly = 1) { con.requeue(msg.id) }
        confirmVerified(con)
    }

    @Test
    fun testTouchAfterFinish() {
        msg.finish()
        msg.touch()
        assert(msg.isFinished)
        assert(!msg.isRequeued)
        verify(exactly = 1) { con.finish(msg.id) }
        confirmVerified(con)
    }

    @Test
    fun testGetTopic() {
        every { con.topic } returns "TOPIC"
        assertEquals("TOPIC", msg.topic)
    }

    @Test
    fun testBadConnectionType() {
        val con: Connection = mockk(relaxed = true)
        val msg = Message(System.nanoTime(), 0, "blabla", "data".toByteArray(), con)
        assertFailsWith<IllegalStateException> { msg.finish() }
        assertFailsWith<IllegalStateException> { msg.requeue() }
        assertFailsWith<IllegalStateException> { msg.touch() }
        assertFailsWith<IllegalStateException> { msg.topic }
    }
}