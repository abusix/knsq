package com.abusix.knsq.subscribe

import com.abusix.knsq.connection.SubConnection
import com.abusix.knsq.protocol.Message
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import java.time.Duration
import java.time.Instant
import java.util.concurrent.Executors
import kotlin.math.abs
import kotlin.test.Test
import kotlin.test.assertEquals


class BackoffHandlerTest {
    private val handleDelays = mutableListOf<Duration>()
    private var lastHandleTime: Instant = Instant.now()
    private val handleSucceedQueue = mutableListOf(true, false, false, false, true, true, true, true)

    @Synchronized
    private fun onMessage(msg: Message) {
        val now = Instant.now()
        val delay = Duration.between(lastHandleTime, now)
        handleDelays.add(delay)
        lastHandleTime = now
        println("delay: ${delay.toMillis()}; Msg#${msg.id}")
        if (handleSucceedQueue.isNotEmpty() && !handleSucceedQueue.removeFirst()) {
            throw RuntimeException("test backoff")
        }
    }

    @Test
    fun testHandler() {
        val sub = mockk<Subscription>(relaxed = true)
        val executor = Executors.newSingleThreadScheduledExecutor()
        every { sub.scheduledExecutor } returns executor
        val con = mockk<SubConnection>(relaxed = true)

        var maxInFlight = 1
        val slot = slot<Int>()
        every { sub.maxInFlight = capture(slot) } answers {
            maxInFlight = slot.captured
        }

        every { sub.maxInFlight } returns 1
        every { sub.running } returns true

        val msgs = (1..10).map { Message(0, 0, it.toString(), "Message $it".toByteArray(), con) }
        val finished = mutableListOf<String>()
        val queue = msgs.toMutableList()

        val slot2 = slot<String>()
        every { con.requeue(capture(slot2)) } answers {
            println("REQ ${slot2.captured}")
            val msg = msgs[slot2.captured.toInt()]
            queue.add(Message(0, msg.attempts + 1, msg.id, msg.data, con))

        }
        every { con.finish(capture(slot2)) } answers {
            println("FIN ${slot2.captured}")
            finished.add(slot2.captured)
        }

        val handler = BackoffHandler(sub, this::onMessage)

        lastHandleTime = Instant.now()

        while (finished.size < msgs.size) {
            if (queue.isEmpty() || maxInFlight == 0) {
                Thread.sleep(100)
            } else if (msgs.size - (queue.size + finished.size) >= maxInFlight) {
                Thread.sleep(0)
            } else {
                val el = queue.removeFirst()
                Thread { handler.handle(el) }.start()
            }
        }
        val realDelays = handleDelays.map { it.toMillis() }
        val expectedDelays = listOf(0, 0, 1000, 2000, 4000, 2000, 1000, 0, 0, 0, 0, 0, 0)
        println("delays: $realDelays")

        assertEquals(handleDelays.size, realDelays.size)
        for (i in handleDelays.indices) {
            assert(abs(realDelays[i] - expectedDelays[i]) < 500)
        }
    }
}