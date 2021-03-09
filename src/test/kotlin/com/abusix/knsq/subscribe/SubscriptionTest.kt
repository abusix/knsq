package com.abusix.knsq.subscribe

import com.abusix.knsq.config.ClientConfig
import com.abusix.knsq.config.ConnectionConfig
import com.abusix.knsq.connection.SubConnection
import com.google.common.collect.Sets
import com.google.common.net.HostAndPort
import io.mockk.every
import io.mockk.mockk
import io.mockk.unmockkAll
import io.mockk.verify
import java.time.Duration
import java.time.Instant
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledFuture
import kotlin.reflect.KMutableProperty
import kotlin.reflect.full.declaredFunctions
import kotlin.reflect.full.memberProperties
import kotlin.reflect.jvm.isAccessible
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse

@Suppress("UnstableApiUsage")
class SubscriptionTest {
    @AfterTest
    fun cleanup() {
        unmockkAll()
    }

    @Test
    fun testDistributeInFlight() {
        val config = ClientConfig()
        val subscriber = mockk<Subscriber>(relaxed = true)
        every { subscriber.initialMaxInFlight } returns 200
        every { subscriber.lookupInterval } returns Duration.ofSeconds(5)

        var sub = Subscription(config, "topic", "channel", subscriber, true)
        testInFlight(1, 1, sub, 200)
        sub = Subscription(config, "topic", "channel", subscriber, true)
        testInFlight(2, 2, sub, 100, 100)
        sub = Subscription(config, "topic", "channel", subscriber, true)
        testInFlight(2, 1, sub, 199)
        sub = Subscription(config, "topic", "channel", subscriber, true)
        testInFlight(3, 3, sub, 66, 67, 67)
        sub = Subscription(config, "topic", "channel", subscriber, true)
        testInFlight(5, 3, sub, 66, 66, 66)
        every { subscriber.initialMaxInFlight } returns 2500
        sub = Subscription(config, "topic", "channel", subscriber, true)
        testInFlight(4, 4, sub, 625, 625, 625, 625)
        sub = Subscription(config, "topic", "channel", subscriber, true)
        testInFlight(6, 6, sub, 417, 417, 417, 417, 416, 416)
        sub = Subscription(config, "topic", "channel", subscriber, true)
        testInFlight(7, 4, sub, 624, 624, 624, 625)
        sub = Subscription(config, "topic", "channel", subscriber, true)
        testInFlight(10, 6, sub, 416, 416, 416, 416, 416, 416)
    }

    private fun setupConMap(numCons: Int, sub: Subscription): MutableMap<HostAndPort, SubConnection> {
        @Suppress("UNCHECKED_CAST")
        val conMap = Subscription::class.memberProperties.first { it.name == "connectionMap" }
            .apply { isAccessible = true }.getter.call(sub) as MutableMap<HostAndPort, SubConnection>
        for (i in 1..numCons) {
            val mock = mockk<SubConnection>(relaxed = true)
            val now = Instant.now()
            val host = HostAndPort.fromParts("localhost", i)
            conMap[host] = mock
            every { mock.config } returns ConnectionConfig(
                version = "", tls = false, deflate = false,
                maxDeflateLevel = 0, snappy = false, authRequired = false, deflateLevel = 0
            )
            every { mock.lastActionFlush } returns now
            every { mock.setMaxInFlight(any(), any()) } answers {
                every { mock.maxInFlight } returns firstArg()
            }
            every { mock.host } returns host
        }
        return conMap
    }

    private fun testInFlight(numCons: Int, numActive: Int, sub: Subscription, vararg expectedMaxInFlight: Int) {
        val conMap = setupConMap(numCons, sub)
        sub.updateConnections(conMap.keys)
        val allCons = conMap.values.toSet()
        val active = setActive(numActive, allCons)
        val inactive = Sets.difference(allCons, active)
        sub.updateConnections(conMap.keys)
        assertFalse(sub.isLowFlight)
        assertEquals(numCons, conMap.size)
        assertEquals(Sets.union(active, inactive), conMap.values.toSet())
        for (con in inactive) {
            assertEquals(1, con.maxInFlight)
        }
        assertEquals(expectedMaxInFlight.toList().sorted(), active.map { it.maxInFlight }.sorted())
    }

    private fun setActive(numActive: Int, cons: Set<SubConnection>): Set<SubConnection> {
        val activeSet = mutableSetOf<SubConnection>()
        val recent = Instant.now() - Duration.ofSeconds(1)
        for (con in cons) {
            if (activeSet.size < numActive) {
                every { con.lastActionFlush } returns recent
                activeSet.add(con)
            } else {
                every { con.lastActionFlush } returns Instant.EPOCH
            }
        }
        return activeSet
    }

    @Test
    fun testLowFlight() {
        val config = ClientConfig()
        val subscriber = mockk<Subscriber>(relaxed = true)
        every { subscriber.initialMaxInFlight } returns 200
        every { subscriber.lookupInterval } returns Duration.ofSeconds(5)
        var sub = Subscription(config, "topic", "channel", subscriber, true)
        testLowFlight(sub, 2, 3)
        sub = Subscription(config, "topic", "channel", subscriber, true)
        testLowFlight(sub, 1, 2)
        sub = Subscription(config, "topic", "channel", subscriber, true)
        testLowFlight(sub, 5, 10)
    }

    private fun testLowFlight(sub: Subscription, maxInflight: Int, numCons: Int) {
        val conMap = setupConMap(numCons, sub)
        sub.maxInFlight = maxInflight
        sub.updateConnections(conMap.keys)
        assert(sub.isLowFlight)
        val ready = conMap.values.filter { it.maxInFlight != 0 }.toSet()
        val paused = Sets.difference(conMap.values.toSet(), ready)
        assertEquals(maxInflight, ready.size)
        assertEquals(numCons - maxInflight, paused.size)

        Subscription::class.declaredFunctions.first { it.name == "rotateLowFlight" }
            .apply { isAccessible = true }.call(sub)

        val nextReady = conMap.values.filter { it.maxInFlight != 0 }.toSet()
        val nextPaused = Sets.difference(conMap.values.toSet(), nextReady)
        assertEquals(maxInflight, nextReady.size)
        assertEquals(numCons - maxInflight, nextPaused.size)
        assertEquals(1, Sets.difference(ready, nextReady).size)
        assertEquals(1, Sets.difference(paused, nextPaused).size)
    }

    @Test
    fun testStop() {
        val subscriberMock = mockk<Subscriber>(relaxed = true)
        val executor = Executors.newSingleThreadScheduledExecutor()
        every { subscriberMock.scheduledExecutor } returns executor
        val sub = Subscription(mockk(), "", "", subscriberMock, true)
        val conMap = setupConMap(2, sub)
        val taskMock = mockk<ScheduledFuture<*>>(relaxed = true)
        Subscription::class.memberProperties.first { it.name == "lowFlightRotateTask" }.apply {
            @Suppress("UNCHECKED_CAST")
            this as KMutableProperty<ScheduledFuture<*>>
            isAccessible = true
            setter.call(sub, taskMock)
        }
        sub.stop()
        verify { taskMock.cancel(false) }
        conMap.values.forEach {
            verify { it.stop() }
        }
        assert(!executor.isShutdown) // Connection MUST NOT shutdown the executor
    }
}