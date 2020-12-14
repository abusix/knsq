package com.abusix.knsq.connection

import com.abusix.knsq.config.ClientConfig
import com.abusix.knsq.config.ConnectionConfig
import com.abusix.knsq.protocol.Message
import com.google.common.net.HostAndPort
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import org.junit.Assert.assertArrayEquals
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import java.time.Duration
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertFailsWith

@Suppress("UnstableApiUsage")
class PubConnectionTest {
    private val clientConfig = ClientConfig()
    private val connectionConfig = ConnectionConfig(
        version = "", deflate = false, tls = false,
        snappy = false, authRequired = true, maxDeflateLevel = 0, heartbeatInterval = 10, deflateLevel = 0
    )
    private val con = spyk(
        PubConnection(clientConfig, HostAndPort.fromHost("localhost"), mockk()),
        recordPrivateCalls = true
    )

    private val output = ByteArrayOutputStream()
    private val dataOutput = DataOutputStream(output)

    @BeforeTest
    fun setup() {
        con.setPrivateProperty("output", dataOutput)
        con.setPrivateProperty("config", connectionConfig)
    }

    @Test
    fun testPublish() {
        every { con["flushAndReadOK"]() } returns Unit
        con.publish("topic", "message".toByteArray())
        assertArrayEquals(
            "PUB topic\n".toByteArray() + byteArrayOf(0, 0, 0, 7) + "message".toByteArray(),
            output.toByteArray()
        )
    }

    @Test
    fun testPublishDeferred() {
        every { con["flushAndReadOK"]() } returns Unit
        con.publishDeferred("topic", "message".toByteArray(), Duration.ofMillis(5000))
        assertArrayEquals(
            "DPUB topic 5000\n".toByteArray() + byteArrayOf(0, 0, 0, 7) + "message".toByteArray(),
            output.toByteArray()
        )
    }

    @Test
    fun testPublishMultiple() {
        every { con["flushAndReadOK"]() } returns Unit
        con.publishMultiple("topic", listOf("message".toByteArray(), "message2".toByteArray()))
        assertArrayEquals(
            "MPUB topic\n".toByteArray() + byteArrayOf(0, 0, 0, 27, 0, 0, 0, 2)
                    + byteArrayOf(0, 0, 0, 7) + "message".toByteArray()
                    + byteArrayOf(0, 0, 0, 8) + "message2".toByteArray(),
            output.toByteArray()
        )
    }

    @Test
    fun testOnIncomingMessage() {
        val msg = Message(0, 0, "", byteArrayOf(), con)
        assertFailsWith<Exception> { con.onIncomingMessage(msg) }
    }

}