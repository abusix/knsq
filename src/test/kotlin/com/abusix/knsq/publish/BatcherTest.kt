package com.abusix.knsq.publish

import io.mockk.confirmVerified
import io.mockk.mockk
import io.mockk.verify
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import kotlin.test.Test

class BatcherTest {

    private val pub = mockk<Publisher>(relaxed = true)
    private val msg1 = "a".repeat(30).toByteArray()
    private val msg2 = "b".repeat(30).toByteArray()

    @Test
    fun testMaxSize() {
        val executor = Executors.newSingleThreadScheduledExecutor()
        val batcher = Batcher(pub, executor, "TOPIC", maxSize = 50, maxDelay = Duration.ofDays(1000))
        batcher.publish(msg1)
        Thread.sleep(1)
        confirmVerified(pub)
        batcher.publish(msg2)
        Thread.sleep(1)
        verify(exactly = 1) { pub.publishMultiple("TOPIC", any()) }
        confirmVerified(pub)
        executor.shutdownNow()
    }

    @Test
    fun testMaxSizeWithNothingAfter() {
        val executor = Executors.newSingleThreadScheduledExecutor()
        val batcher = Batcher(pub, executor, "TOPIC", maxSize = 50, maxDelay = Duration.ofMillis(2000))
        batcher.publish(msg1)
        Thread.sleep(1)
        confirmVerified(pub)
        batcher.publish(msg2)
        Thread.sleep(1)
        verify(exactly = 1) { pub.publishMultiple("TOPIC", any()) }
        Thread.sleep(2001)
        verify(exactly = 2) { pub.publishMultiple("TOPIC", any()) }
        confirmVerified(pub)
        executor.shutdownNow()
    }

    @Test
    fun testMaxDelay() {
        val executor = Executors.newSingleThreadScheduledExecutor()
        val batcher = Batcher(pub, executor, "TOPIC", maxSize = 100000, maxDelay = Duration.ofSeconds(1))
        batcher.publish(msg1)
        confirmVerified(pub)
        batcher.publish(msg2)
        confirmVerified(pub)
        Thread.sleep(1100)
        verify(exactly = 1) { pub.publishMultiple("TOPIC", any()) }
        confirmVerified(pub)
        executor.shutdownNow()
    }

    @Test
    fun testEmptyBatch() {
        val executor = mockk<ScheduledExecutorService>(relaxed = true)
        val batcher = Batcher(pub, executor, "TOPIC")
        batcher.sendBatches()
        confirmVerified(pub)
    }

    @Test
    fun testSendBatchWithHalfFull() {
        val executor = mockk<ScheduledExecutorService>(relaxed = true)
        val batcher = Batcher(pub, executor, "TOPIC", maxSize = 100000, maxDelay = Duration.ofDays(1000))
        batcher.publish(msg1)
        confirmVerified(pub)
        batcher.publish(msg2)
        confirmVerified(pub)
        batcher.sendBatches()
        confirmVerified(pub)
    }

    @Test
    fun testSendBatchWithHalfFullAndSendThem() {
        val executor = mockk<ScheduledExecutorService>(relaxed = true)
        val batcher = Batcher(pub, executor, "TOPIC", maxSize = 100000, maxDelay = Duration.ofDays(1000))
        batcher.publish(msg1)
        confirmVerified(pub)
        batcher.publish(msg2)
        confirmVerified(pub)
        batcher.sendBatches(true)
        verify(exactly = 1) { pub.publishMultiple("TOPIC", any()) }
        confirmVerified(pub)
    }

    @Test
    fun testJobAfterImmediateSendBatch() {
        val executor = Executors.newSingleThreadScheduledExecutor()
        val batcher = Batcher(pub, executor, "TOPIC", maxSize = 100000, maxDelay = Duration.ofSeconds(1))
        batcher.publish(msg1)
        confirmVerified(pub)
        batcher.publish(msg2)
        confirmVerified(pub)
        batcher.sendBatches(true)
        verify(exactly = 1) { pub.publishMultiple("TOPIC", any()) }
        confirmVerified(pub)
        Thread.sleep(1001)
        confirmVerified(pub)
        executor.shutdownNow()
    }
}