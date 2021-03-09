package com.abusix.knsq.connection

import com.abusix.knsq.config.ClientConfig
import com.abusix.knsq.config.ConnectionConfig
import com.abusix.knsq.protocol.Message
import com.abusix.knsq.subscribe.Subscriber
import com.abusix.knsq.subscribe.Subscription
import com.google.common.net.HostAndPort
import io.mockk.*
import java.io.*
import java.lang.reflect.InvocationTargetException
import java.time.Duration
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.reflect.KMutableProperty
import kotlin.reflect.full.memberFunctions
import kotlin.reflect.full.memberProperties
import kotlin.reflect.jvm.isAccessible
import kotlin.test.*

@Suppress("UnstableApiUsage")
class SubConnectionTest {
    private val clientConfig = ClientConfig()
    private val connectionConfig = ConnectionConfig(
        version = "", deflate = false, tls = false,
        snappy = false, authRequired = true, maxDeflateLevel = 0, heartbeatInterval = 10, deflateLevel = 0
    )
    private val subscriber = Subscriber(clientConfig = clientConfig, maxFlushDelay = Duration.ofMillis(500))
    private val subscription = spyk(Subscription(clientConfig, "topic", "channel", subscriber, true))
    private val con = spyk(
        SubConnection(
            clientConfig, HostAndPort.fromHost("localhost"),
            subscription
        ), recordPrivateCalls = true
    )

    private val output = ByteArrayOutputStream()
    private val dataOutput = DataOutputStream(output)

    @BeforeTest
    fun setup() {
        con.setPrivateProperty("output", dataOutput)
        con.setPrivateProperty("config", connectionConfig)
        subscriber.maxMessageAttempts = 5
        con.getPrivateProperty<ScheduledExecutorService>("executor").shutdownNow()
    }

    @Test
    fun testOnIncomingMessage() {
        var msg: Message? = null
        var failedMsg: Message? = null
        val toProcess = Message(0, 0, "ID", byteArrayOf(), con)
        val lock = ReentrantLock()
        lock.withLock {
            val cond = lock.newCondition()
            con.onMessage = {
                msg = it
                lock.withLock { cond.signalAll() }
            }
            con.onFailedMessage = {
                failedMsg = it
                lock.withLock { cond.signalAll() }
            }

            con.onIncomingMessage(toProcess)
            cond.await()
        }
        assertSame(toProcess, msg)
        assertNull(failedMsg)
        assert(output.toByteArray().isEmpty())
    }

    @Test
    fun testOnIncomingMessageTooManyAttempts() {
        con.setPrivateProperty("isRunning", true)
        var msg: Message? = null
        var failedMsg: Message? = null
        val toProcess = Message(0, 10, "ID", byteArrayOf(), con)
        val lock = ReentrantLock()
        lock.withLock {
            val cond = lock.newCondition()
            con.onMessage = {
                msg = it
                lock.withLock { cond.signalAll() }
            }
            con.onFailedMessage = {
                failedMsg = it
                lock.withLock { cond.signalAll() }
            }

            con.onIncomingMessage(toProcess)
            cond.await()
        }
        assertSame(toProcess, failedMsg)
        assertNull(msg)
        assertEquals("FIN ID\n", output.toByteArray().decodeToString())
    }

    @Test
    fun testFinish() {
        con.setPrivateProperty("isRunning", true)
        con.finish("AID")
        assertEquals("FIN AID\n", output.toByteArray().decodeToString())
        verify(exactly = 0) { con.stop() }
    }

    @Test
    fun testFinishWithIOException() {
        con.setPrivateProperty("isRunning", true)
        every { con["writeCommand"](any<String>(), anyVararg<Any>()) } throws IOException()
        every { con.stop() } just Runs
        con.finish("AID")
        verify { con.stop() }
    }

    @Test
    fun testFinishWithOtherException() {
        con.setPrivateProperty("isRunning", true)
        every { con["writeCommand"](any<String>(), anyVararg<Any>()) } throws Exception()
        assertFailsWith<Exception> { con.finish("AID") }
    }

    @Test
    fun testFinishAfterStopping() {
        every { con.stop() } just Runs
        con.setPrivateProperty("inFlight", 1)
        con.finish("AID")
        assertEquals(0, output.size())
    }

    @Test
    fun testRequeue() {
        con.setPrivateProperty("isRunning", true)
        con.requeue("RID")
        assertEquals("REQ RID 0\n", output.toByteArray().decodeToString())
        verify(exactly = 0) { con.stop() }
    }

    @Test
    fun testRequeueWithIOException() {
        con.setPrivateProperty("isRunning", true)
        every { con["writeCommand"](any<String>(), anyVararg<Any>()) } throws IOException()
        every { con.stop() } just Runs
        con.requeue("RID")
        verify { con.stop() }
    }

    @Test
    fun testRequeueWithOtherException() {
        con.setPrivateProperty("isRunning", true)
        every { con["writeCommand"](any<String>(), anyVararg<Any>()) } throws Exception()
        assertFailsWith<Exception> { con.requeue("RID") }
    }

    @Test
    fun testRequeueAfterStopping() {
        every { con.stop() } just Runs
        con.setPrivateProperty("inFlight", 1)
        con.requeue("RID")
        assertEquals(0, output.size())
    }

    @Test
    fun testTouch() {
        con.setPrivateProperty("isRunning", true)
        con.touch("TID")
        assertEquals("TOUCH TID\n", output.toByteArray().decodeToString())
        verify(exactly = 0) { con.stop() }
    }

    @Test
    fun testTouchWithIOException() {
        con.setPrivateProperty("isRunning", true)
        every { con["writeCommand"](any<String>(), anyVararg<Any>()) } throws IOException()
        every { con.stop() } just Runs
        con.touch("TID")
        verify { con.stop() }
    }

    @Test
    fun testTouchWithOtherException() {
        con.setPrivateProperty("isRunning", true)
        every { con["writeCommand"](any<String>(), anyVararg<Any>()) } throws Exception()
        assertFailsWith<Exception> { con.touch("TID") }
    }

    @Test
    fun testTouchAfterStopping() {
        every { con.stop() } just Runs
        con.setPrivateProperty("inFlight", 1)
        con.touch("TID")
        assertEquals(0, output.size())
        verify(exactly = 0) { con.stop() }
    }

    @Test
    fun testDelayedFlush() {
        con.setPrivateProperty("unflushedBytes", 2)
        con.invokePrivate<Unit>("delayedFlush")
        assertEquals(0, con.getPrivateProperty("unflushedBytes"))
        val lastActionFlush = con.lastActionFlush
        con.invokePrivate<Unit>("delayedFlush")
        con.setPrivateProperty("unflushedBytes", 2)
        con.invokePrivate<Unit>("delayedFlush")
        assertSame(lastActionFlush, con.lastActionFlush)
        assertEquals(2, con.getPrivateProperty("unflushedBytes"))
        Thread.sleep(500)
        con.invokePrivate<Unit>("delayedFlush")
        assertNotSame(lastActionFlush, con.lastActionFlush)
        assertEquals(0, con.getPrivateProperty("unflushedBytes"))
    }

    @Test
    fun testSetMaxInFlight() {
        val lastActionFlush = con.lastActionFlush
        con.setMaxInFlight(10)
        assertNotSame(lastActionFlush, con.lastActionFlush)
        assertEquals("RDY 10\n", output.toByteArray().decodeToString())
    }

    @Test
    fun testSetMaxInFlightInactive() {
        val lastActionFlush = con.lastActionFlush
        con.setMaxInFlight(10, false)
        assertSame(lastActionFlush, con.lastActionFlush)
        assertEquals("RDY 10\n", output.toByteArray().decodeToString())
    }

    @Test
    fun testStop() {
        var connClosedCalled = false
        every { subscription.connectionClosed(any()) } answers {
            connClosedCalled = true
        }
        con.setPrivateProperty("isRunning", true)
        Connection::class.memberProperties.filterIsInstance<KMutableProperty<*>>()
            .find { it.name == "input" }!!.let {
                it.isAccessible = true
                it.setter.call(con, DataInputStream(ByteArrayInputStream(byteArrayOf())))
            }
        Connection::class.memberProperties.filterIsInstance<KMutableProperty<*>>()
            .find { it.name == "readThread" }!!.let {
                it.isAccessible = true
                it.setter.call(con, mockk<Thread>(relaxed = true))
            }
        con.stop()
        assert(!subscriber.handlerExecutor.isShutdown)
        assert(connClosedCalled)
    }

    @Test
    fun testStopWithInflight() {
        var connClosedCalled = false
        every { con.setMaxInFlight(any()) } returns Unit
        every { subscription.connectionClosed(any()) } answers {
            connClosedCalled = true
        }
        con.setPrivateProperty("isRunning", true)
        con.setPrivateProperty("inFlight", 2)
        Connection::class.memberProperties.filterIsInstance<KMutableProperty<*>>()
            .find { it.name == "input" }!!.let {
                it.isAccessible = true
                it.setter.call(con, DataInputStream(ByteArrayInputStream(byteArrayOf())))
            }
        Connection::class.memberProperties.filterIsInstance<KMutableProperty<*>>()
            .find { it.name == "readThread" }!!.let {
                it.isAccessible = true
                it.setter.call(con, mockk<Thread>(relaxed = true))
            }
        con.stop()
        assert(connClosedCalled)
        verify { con.setMaxInFlight(0) }
    }
}

internal fun SubConnection.setPrivateProperty(prop: String, value: Any) {
    SubConnection::class.memberProperties.filterIsInstance<KMutableProperty<*>>()
        .find { it.name == prop }!!.let {
            it.isAccessible = true
            it.setter.call(this, value)
        }
}

@Suppress("UNCHECKED_CAST")
internal fun <T> SubConnection.getPrivateProperty(prop: String): T {
    return SubConnection::class.memberProperties.find { it.name == prop }!!.let {
        it.isAccessible = true
        it.getter.call(this) as T
    }
}

@Suppress("UNCHECKED_CAST")
internal fun <T> SubConnection.invokePrivate(func: String, vararg params: Any): T {
    try {
        return SubConnection::class.memberFunctions.find { it.name == func }!!.let {
            it.isAccessible = true
            it.call(this, *params) as T
        }
    } catch (e: InvocationTargetException) {
        throw e.cause!!
    }
}