package com.abusix.knsq.publish

import com.abusix.knsq.connection.PubConnection
import io.mockk.*
import java.time.Duration
import java.util.concurrent.ScheduledExecutorService
import kotlin.reflect.full.createType
import kotlin.reflect.full.isSubtypeOf
import kotlin.reflect.full.memberProperties
import kotlin.reflect.jvm.isAccessible
import kotlin.test.*

class PublisherTest {

    @AfterTest
    fun cleanUp() {
        unmockkAll()
    }

    @Test
    fun testOnException() {
        mockkConstructor(PubConnection::class)
        every { anyConstructed<PubConnection>().connect() } returns Unit
        val onExceptionCapture = slot<(Exception) -> Unit>()
        every { anyConstructed<PubConnection>().publish(any(), any()) } returns Unit
        every { anyConstructed<PubConnection>().publishMultiple(any(), any()) } returns Unit
        every { anyConstructed<PubConnection>().onException = capture(onExceptionCapture) } returns Unit
        val pub = Publisher("Test")
        pub.publish("bla", "bla".toByteArray())
        assert(onExceptionCapture.isCaptured)
        var called = false
        pub.onException = { called = true }
        onExceptionCapture.captured.invoke(Exception())
        assert(called)
    }

    @Test
    fun testBatcherRetrieval() {
        val pub = Publisher("Test")
        val bat1 = pub.getBatcher("topic1")
        val bat2 = pub.getBatcher("topic2")
        val bat1again = pub.getBatcher("topic1")

        assertSame(bat1, bat1again)
        assertNotSame(bat1, bat2)
    }

    @Test
    fun testPublishBadArguments() {
        val pub = Publisher("Test")
        assertFailsWith<IllegalArgumentException> { pub.publish("a", "".toByteArray()) }
        assertFailsWith<IllegalArgumentException> { pub.publish("", "a".toByteArray()) }
        assertFailsWith<IllegalArgumentException> { pub.publish("a".repeat(65), "a".toByteArray()) }
        assertFailsWith<IllegalArgumentException> { pub.publishMultiple("a", listOf()) }
        assertFailsWith<IllegalArgumentException> { pub.publishMultiple("a", listOf("".toByteArray())) }
        assertFailsWith<IllegalArgumentException> { pub.publishMultiple("", listOf("a".toByteArray())) }
        assertFailsWith<IllegalArgumentException> { pub.publishMultiple("a".repeat(65), listOf("a".toByteArray())) }
        assertFailsWith<IllegalArgumentException> {
            pub.publishMultiple(
                "a",
                listOf("a".toByteArray(), "".toByteArray())
            )
        }
        assertFailsWith<IllegalArgumentException> { pub.publishDeferred("a", "".toByteArray(), Duration.ZERO) }
        assertFailsWith<IllegalArgumentException> { pub.publishDeferred("", "a".toByteArray(), Duration.ZERO) }
        assertFailsWith<IllegalArgumentException> {
            pub.publishDeferred(
                "a".repeat(65),
                "a".toByteArray(),
                Duration.ZERO
            )
        }
        assertFailsWith<IllegalArgumentException> {
            pub.publishDeferred(
                "a",
                "a".toByteArray(),
                Duration.ofSeconds(-1)
            )
        }
        assertFailsWith<IllegalArgumentException> { pub.publishBuffered("a", "".toByteArray()) }
        assertFailsWith<IllegalArgumentException> { pub.publishBuffered("", "a".toByteArray()) }
        assertFailsWith<IllegalArgumentException> { pub.publishBuffered("a".repeat(65), "a".toByteArray()) }
    }

    @Test
    fun testPublish() {
        mockkConstructor(PubConnection::class)
        every { anyConstructed<PubConnection>().publish(any(), any()) } returns Unit
        every { anyConstructed<PubConnection>().connect() } returns Unit
        val pub = Publisher("Test")
        pub.publish("bla", "data".toByteArray())
        verify {
            anyConstructed<PubConnection>().publish("bla", "data".toByteArray())
        }
    }

    @Test
    fun testPublishList() {
        mockkConstructor(PubConnection::class)
        every { anyConstructed<PubConnection>().publishMultiple(any(), any()) } returns Unit
        every { anyConstructed<PubConnection>().connect() } returns Unit
        val pub = Publisher("Test")
        val msgs = listOf("data".toByteArray())
        pub.publishMultiple("bla", msgs)
        verify {
            anyConstructed<PubConnection>().publishMultiple("bla", msgs)
        }
    }

    @Test
    fun testPublishDeferred() {
        mockkConstructor(PubConnection::class)
        every { anyConstructed<PubConnection>().publishDeferred(any(), any(), any()) } returns Unit
        every { anyConstructed<PubConnection>().connect() } returns Unit
        val pub = Publisher("Test")
        pub.publishDeferred("bla", "data".toByteArray(), Duration.ofSeconds(3))
        verify { anyConstructed<PubConnection>().publishDeferred("bla", "data".toByteArray(), Duration.ofSeconds(3)) }
    }

    @Test
    fun testPublishBuffered() {
        mockkConstructor(Batcher::class)
        every { anyConstructed<Batcher>().publish(any()) } returns Unit
        val pub = Publisher("Test")
        pub.publishBuffered("bla", "data".toByteArray())
        verify { anyConstructed<Batcher>().publish("data".toByteArray()) }
    }

    @Test
    fun testStop() {
        mockkConstructor(Batcher::class)
        mockkConstructor(PubConnection::class)
        every { anyConstructed<Batcher>().sendBatches() } returns Unit
        every { anyConstructed<PubConnection>().publish(any(), any()) } returns Unit
        every { anyConstructed<PubConnection>().connect() } returns Unit
        every { anyConstructed<PubConnection>().stop() } just Runs
        val pub = Publisher("Test")
        pub.getBatcher("bla")
        pub.publish("bla", "data".toByteArray())
        pub.stop()
        verify {
            anyConstructed<Batcher>().sendBatches()
            anyConstructed<PubConnection>().stop()
        }
        assert(pub::class.memberProperties
            .filter { it.returnType.isSubtypeOf(ScheduledExecutorService::class.createType()) }.all {
                it.isAccessible = true
                val x = it.getter.call(pub) as ScheduledExecutorService
                x.isShutdown
            }
        )
    }

    @Test
    fun testIsAliveInitial() {
        val pub = Publisher("Test")
        assert(pub.isAlive())
    }

    @Test
    fun testIsAliveAfterConnectionRetrieval() {
        mockkConstructor(PubConnection::class)
        every { anyConstructed<PubConnection>().isRunning } returns true
        every { anyConstructed<PubConnection>().publish(any(), any()) } returns Unit
        every { anyConstructed<PubConnection>().connect() } returns Unit
        val pub = Publisher("Test")
        pub.publish("bla", "data".toByteArray())
        assert(pub.isAlive())
    }

    @Test
    fun testIsAliveAfterConnectionClosed() {
        mockkConstructor(PubConnection::class)
        every { anyConstructed<PubConnection>().isRunning } returns false
        every { anyConstructed<PubConnection>().publish(any(), any()) } returns Unit
        every { anyConstructed<PubConnection>().connect() } returns Unit
        val pub = Publisher("Test")
        pub.publish("bla", "data".toByteArray())
        assert(!pub.isAlive())
    }

    @Test
    fun testConnect() {
        mockkConstructor(PubConnection::class)
        every { anyConstructed<PubConnection>().connect() } returns Unit
        val pub = Publisher("Test")
        pub.connect()
        verify {
            anyConstructed<PubConnection>().connect()
        }
    }
}