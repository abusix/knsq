package com.abusix.knsq.connection

import com.abusix.knsq.common.KNSQException
import com.abusix.knsq.common.NSQException
import com.abusix.knsq.config.ClientConfig
import com.abusix.knsq.config.ConnectionConfig
import com.abusix.knsq.protocol.Error
import com.abusix.knsq.protocol.ErrorType
import com.abusix.knsq.protocol.Message
import com.abusix.knsq.protocol.Response
import com.google.common.net.HostAndPort
import io.mockk.mockk
import io.mockk.spyk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertArrayEquals
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.lang.reflect.InvocationTargetException
import java.time.Duration
import java.time.Instant
import java.util.concurrent.Executors
import kotlin.reflect.KMutableProperty
import kotlin.reflect.full.memberFunctions
import kotlin.reflect.full.memberProperties
import kotlin.reflect.jvm.isAccessible
import kotlin.test.*

@Suppress("UnstableApiUsage")
class ConnectionTest {
    private val clientConfig = ClientConfig()
    private val connectionConfig = ConnectionConfig(
        version = "", deflate = false, tls = false,
        snappy = false, authRequired = true, maxDeflateLevel = 0, heartbeatInterval = 10, deflateLevel = 0
    )
    private val executor = Executors.newScheduledThreadPool(1)
    private var onIncomingMessage: (Message) -> Unit = { throw Exception() }
    private val con = spyk(object : Connection(clientConfig, HostAndPort.fromHost("localhost"), executor) {
        override fun onIncomingMessage(message: Message) {
            this@ConnectionTest.onIncomingMessage(message)
        }

        fun getResponseQueuePublic() = responseQueue
    }, recordPrivateCalls = true)
    private val output = ByteArrayOutputStream()
    private val dataOutput = DataOutputStream(output)
    private val readThread = mockk<Thread>(relaxed = true)

    @BeforeTest
    fun setup() {
        setInput(byteArrayOf())
        con.setPrivateProperty("output", dataOutput)
        con.setPrivateProperty("config", connectionConfig)
        con.setPrivateProperty("readThread", readThread)
    }

    private fun setInput(data: ByteArray) {
        val input = DataInputStream(ByteArrayInputStream(data))
        con.setPrivateProperty("input", input)
    }

    @Test
    fun testWriteCommand() {
        con.invokePrivate<Unit>("writeCommand", "test", emptyArray<Any>())
        dataOutput.flush()
        assertArrayEquals("test\n".toByteArray(), output.toByteArray())
    }

    @Test
    fun testWriteCommandWithSingleParam() {
        con.invokePrivate<Unit>("writeCommand", "test", arrayOf("param1"))
        dataOutput.flush()
        assertArrayEquals("test param1\n".toByteArray(), output.toByteArray())
    }

    @Test
    fun testWriteCommandWithTwoParams() {
        con.invokePrivate<Unit>("writeCommand", "test", arrayOf("param1", 2))
        dataOutput.flush()
        assertArrayEquals("test param1 2\n".toByteArray(), output.toByteArray())
    }

    @Test
    fun testWriteData() {
        con.invokePrivate<Unit>("writeData", "hello".toByteArray())
        dataOutput.flush()
        assertArrayEquals(byteArrayOf(0, 0, 0, 5) + "hello".toByteArray(), output.toByteArray())
    }

    @Test
    fun testStop() {
        val input = spyk(DataInputStream(ByteArrayInputStream(byteArrayOf())))
        val output = spyk(DataOutputStream(ByteArrayOutputStream()))
        con.setPrivateProperty("isRunning", true)
        con.setPrivateProperty("input", input)
        con.setPrivateProperty("output", output)
        con.stop()
        verify {
            input.close()
            output.close()
            readThread.interrupt()
        }
        assertFalse { con.isRunning }
        assert(!executor.isShutdown) // Connection MUST NOT shutdown the executor
    }

    @Test
    fun testSendAuthorizationSuccess() {
        val response = byteArrayOf(0, 0, 0, 6, 0, 0, 0, 0) + "{}".toByteArray()
        setInput(response)
        clientConfig.authSecret = "bla".toByteArray()
        con.invokePrivate<Unit>("sendAuthorization")
        dataOutput.flush()
        assertArrayEquals("AUTH\n".toByteArray() + byteArrayOf(0, 0, 0, 3) + "bla".toByteArray(), output.toByteArray())
    }

    @Test
    fun testSendAuthorizationError() {
        val response = byteArrayOf(0, 0, 0, 4, 0, 0, 0, 1)
        setInput(response)
        clientConfig.authSecret = "bla".toByteArray()
        assertFailsWith<NSQException> { con.invokePrivate<Unit>("sendAuthorization") }
        dataOutput.flush()
        assertArrayEquals("AUTH\n".toByteArray() + byteArrayOf(0, 0, 0, 3) + "bla".toByteArray(), output.toByteArray())
    }

    @Test
    fun testSendAuthorizationWithoutNeed() {
        val config = ConnectionConfig(
            version = "", deflate = false, tls = false,
            snappy = false, authRequired = false, maxDeflateLevel = 0, deflateLevel = 0
        )
        con.setPrivateProperty("config", config)
        con.invokePrivate<Unit>("sendAuthorization")
        dataOutput.flush()
        assert(output.toByteArray().isEmpty())
    }

    @Test
    fun testCheckHeartbeat() {
        con.setPrivateProperty("isRunning", true)
        connectionConfig.heartbeatInterval = 500
        con.setPrivateProperty("lastHeartbeat", Instant.now())
        con.invokePrivate<Unit>("checkHeartbeat")
        assert(con.isRunning)
        Thread.sleep(1000)
        con.invokePrivate<Unit>("checkHeartbeat")
        assert(!con.isRunning)
    }

    @Test
    fun testReadFrameResponse() {
        val response = byteArrayOf(0, 0, 0, 4, 0, 0, 0, 0)
        setInput(response)
        val frame: Response = con.invokePrivate("readFrame")
        assertEquals(4, frame.size)
        assert(frame.msg.isEmpty())
    }

    @Test
    fun testReadFrameResponseWithPayload() {
        val response = byteArrayOf(0, 0, 0, 21, 0, 0, 0, 0) + "Hi I am a message".toByteArray()
        setInput(response)
        val frame: Response = con.invokePrivate("readFrame")
        assertEquals(21, frame.size)
        assertEquals("Hi I am a message", frame.msg)
    }

    @Test
    fun testReadFrameError() {
        val response = byteArrayOf(0, 0, 0, 4, 0, 0, 0, 1)
        setInput(response)
        val frame: Error = con.invokePrivate("readFrame")
        assertEquals(4, frame.size)
        assertEquals(ErrorType.E_INVALID, frame.errorType)
    }

    @Test
    fun testReadFrameErrorWithContent() {
        val response = byteArrayOf(0, 0, 0, 14, 0, 0, 0, 1) + ErrorType.E_BAD_BODY.name.toByteArray()
        setInput(response)
        val frame: Error = con.invokePrivate("readFrame")
        assertEquals(14, frame.size)
        assertEquals(ErrorType.E_BAD_BODY, frame.errorType)
    }

    @Test
    fun testReadFrameMessage() {
        val response = byteArrayOf(0, 0, 0, 38, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1) +
                "a".repeat(16).toByteArray() + "Message!".toByteArray()
        setInput(response)
        val frame: Message = con.invokePrivate("readFrame")
        assertEquals(38, frame.size)
        assertEquals(1, frame.attempts)
        assertEquals("a".repeat(16), frame.id)
        assertEquals(Instant.EPOCH, frame.timestamp)
        assertEquals("Message!", frame.data.decodeToString())
    }

    @Test
    fun testRead() {
        val response = byteArrayOf(0, 0, 0, 4, 0, 0, 0, 0)
        setInput(response)
        con.setPrivateProperty("isRunning", true)
        lateinit var exc: Exception
        con.onException = { exc = it }
        assert(con.isRunning)
        con.invokePrivate<Unit>("read")
        val frame = con.getResponseQueuePublic().take()
        assert(frame is Response)
        exc.message //test for lateinit is initialized
        assert(!con.isRunning)
        verify { con.stop() }
    }

    @Test
    fun testReadInterruptException() {
        val response = byteArrayOf(0, 0, 0, 38, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1) +
                "a".repeat(16).toByteArray() + "Message!".toByteArray()
        setInput(response)
        con.setPrivateProperty("isRunning", true)
        var exc: Exception? = null
        con.onException = { exc = it }
        onIncomingMessage = { throw InterruptedException() }
        assert(con.isRunning)
        con.invokePrivate<Unit>("read")
        assert(con.getResponseQueuePublic().isEmpty())
        assertNull(exc)
        assert(!con.isRunning)
        verify { con.stop() }
    }

    @Test
    fun testReadErrorNonTerminating() {
        val response1 = byteArrayOf(0, 0, 0, 16, 0, 0, 0, 1) + ErrorType.E_REQ_FAILED.name.toByteArray()
        val response2 = byteArrayOf(0, 0, 0, 38, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1) +
                "a".repeat(16).toByteArray() + "Message!".toByteArray()
        setInput(response1 + response2)
        con.setPrivateProperty("isRunning", true)
        val excs = mutableListOf<Exception>()
        con.onException = { excs.add(it) }
        lateinit var nsqError: Error
        con.onNSQError = { nsqError = it }
        lateinit var frame2: Message
        onIncomingMessage = { it -> frame2 = it }
        assert(con.isRunning)
        con.invokePrivate<Unit>("read")
        val frame1 = con.getResponseQueuePublic().take()
        assert(frame1 is Error)
        assertEquals("Message!", frame2.data.decodeToString())
        assertEquals(1, excs.size)
        assert(excs[0] !is KNSQException)
        assertSame(frame1, nsqError)
        assert(!con.isRunning)
        verify { con.stop() }
    }

    @Test
    fun testReadErrorTerminating() {
        val response1 = byteArrayOf(0, 0, 0, 13, 0, 0, 0, 1) + ErrorType.E_INVALID.name.toByteArray()
        val response2 = byteArrayOf(0, 0, 0, 38, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1) +
                "a".repeat(16).toByteArray() + "Message!".toByteArray()
        setInput(response1 + response2)
        con.setPrivateProperty("isRunning", true)
        val excs = mutableListOf<Exception>()
        con.onException = { excs.add(it) }
        lateinit var nsqError: Error
        con.onNSQError = { nsqError = it }
        onIncomingMessage = { throw Exception("HEY") }
        assert(con.isRunning)
        con.invokePrivate<Unit>("read")
        val frame1 = con.getResponseQueuePublic().take()
        assert(frame1 is Error)
        assertEquals(0, excs.size)
        assertSame(frame1, nsqError)
        assert(!con.isRunning)
        verify { con.stop() }
    }

    @Test
    fun testReadHeartbeat() {
        val response1 = byteArrayOf(0, 0, 0, 15, 0, 0, 0, 0) + "_heartbeat_".toByteArray()
        val response2 = byteArrayOf(0, 0, 0, 38, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1) +
                "a".repeat(16).toByteArray() + "Message!".toByteArray()
        setInput(response1 + response2)
        con.setPrivateProperty("isRunning", true)
        assert(con.isRunning)
        onIncomingMessage = { Thread.sleep(1000) }
        con.invokePrivate<Unit>("read")
        assert(!con.isRunning)
        assertEquals("NOP\n", output.toByteArray().decodeToString())
        assert(con.getResponseQueuePublic().isEmpty())
        verify { con.stop() }
    }

    @Test
    fun testReceivedHeartbeat() {
        val dataOutput = spyk(DataOutputStream(output))
        con.setPrivateProperty("output", dataOutput)
        val prevLastFlushed = con.lastActionFlush
        con.invokePrivate<Unit>("receivedHeartbeat")
        verify {
            dataOutput.flush()
        }
        assertEquals("NOP\n", output.toByteArray().decodeToString())
        assertEquals(prevLastFlushed, con.lastActionFlush)
        assert(
            Duration.between(Instant.now(), con.getPrivateProperty("lastHeartbeat")).abs()
                    < Duration.ofSeconds(5)
        )
    }

    @Test
    fun testCheckIsOK() {
        val error = Error("")
        val response = Response("Hi!")
        val okResponse = Response("OK")
        val message = Message(0, 1, "", byteArrayOf(), mockk())

        assertFailsWith<NSQException> { con.invokePrivate<Unit>("checkIsOK", error) }
        assertFailsWith<KNSQException> { con.invokePrivate<Unit>("checkIsOK", response) }
        assertFailsWith<KNSQException> { con.invokePrivate<Unit>("checkIsOK", message) }
        con.invokePrivate<Unit>("checkIsOK", okResponse)
    }

    @Test
    fun testFlushAndReadOKTimeout() {
        val dataOutput = spyk(DataOutputStream(output))
        con.setPrivateProperty("output", dataOutput)
        val prevLastFlushed = con.lastActionFlush
        assertFailsWith<KNSQException> { con.invokePrivate<Unit>("flushAndReadOK") }
        verify { dataOutput.flush() }
        assertNotEquals(prevLastFlushed, con.lastActionFlush)
        assertEquals(0, con.getPrivateProperty("unflushedBytes"))
    }

    @Test
    fun testFlushAndReadOKFailure() {
        val dataOutput = spyk(DataOutputStream(output))
        con.setPrivateProperty("output", dataOutput)
        val prevLastFlushed = con.lastActionFlush
        con.getResponseQueuePublic().put(Error(""))
        assertFailsWith<NSQException> { con.invokePrivate<Unit>("flushAndReadOK") }
        verify { dataOutput.flush() }
        assertNotEquals(prevLastFlushed, con.lastActionFlush)
        assertEquals(0, con.getPrivateProperty("unflushedBytes"))
    }

    @Test
    fun testFlushAndReadOKFailure2() {
        val dataOutput = spyk(DataOutputStream(output))
        con.setPrivateProperty("output", dataOutput)
        val prevLastFlushed = con.lastActionFlush
        con.getResponseQueuePublic().put(Response("Heyho"))
        assertFailsWith<KNSQException> { con.invokePrivate<Unit>("flushAndReadOK") }
        verify { dataOutput.flush() }
        assertNotEquals(prevLastFlushed, con.lastActionFlush)
        assertEquals(0, con.getPrivateProperty("unflushedBytes"))
    }

    @Test
    fun testFlushAndReadOKSuccess() {
        val dataOutput = spyk(DataOutputStream(output))
        con.setPrivateProperty("output", dataOutput)
        val prevLastFlushed = con.lastActionFlush
        con.getResponseQueuePublic().put(Response("OK"))
        con.invokePrivate<Unit>("flushAndReadOK")
        verify { dataOutput.flush() }
        assertNotEquals(prevLastFlushed, con.lastActionFlush)
        assertEquals(0, con.getPrivateProperty("unflushedBytes"))
    }

    @Test
    fun testSetStreams() {
        val otherInput = ByteArrayInputStream(byteArrayOf())
        val otherOutput = ByteArrayOutputStream()
        val input = ByteArrayInputStream(byteArrayOf())
        val output = ByteArrayOutputStream()

        val streamPairCls = Connection::class.nestedClasses.first { it.simpleName!!.contains("StreamPair") }
        val pair = streamPairCls.constructors.first().apply { isAccessible = true }.call(otherInput, otherOutput, false)

        con.invokePrivate<Unit>("setStreams", input, output, pair)
        con.getPrivateProperty<DataInputStream>("input")
        con.getPrivateProperty<DataOutputStream>("output")
        val setInput = streamPairCls.memberProperties.first { it.name == "input" }
            .getter.apply { isAccessible = true }.call(pair)
        val setOutput = streamPairCls.memberProperties.first { it.name == "output" }
            .getter.apply { isAccessible = true }.call(pair)
        assertSame(setInput, input)
        assertSame(setOutput, output)
    }
}

internal fun Connection.setPrivateProperty(prop: String, value: Any) {
    Connection::class.memberProperties.filterIsInstance<KMutableProperty<*>>()
        .find { it.name == prop }!!.let {
            it.isAccessible = true
            it.setter.call(this, value)
        }
}

@Suppress("UNCHECKED_CAST")
internal fun <T> Connection.getPrivateProperty(prop: String): T {
    return Connection::class.memberProperties.find { it.name == prop }!!.let {
        it.isAccessible = true
        it.getter.call(this) as T
    }
}

@Suppress("UNCHECKED_CAST")
internal fun <T> Connection.invokePrivate(func: String, vararg params: Any): T {
    try {
        return Connection::class.memberFunctions.find { it.name == func }!!.let {
            it.isAccessible = true
            it.call(this, *params) as T
        }
    } catch (e: InvocationTargetException) {
        throw e.cause!!
    }
}