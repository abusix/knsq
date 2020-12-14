package com.abusix.knsq.subscribe

import com.abusix.knsq.util.KGenericContainer
import org.junit.jupiter.api.Timeout
import org.testcontainers.utility.DockerImageName
import java.util.concurrent.TimeUnit
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals

@Timeout(30, unit = TimeUnit.SECONDS)
class DirectSubscriberIntegrationTest : SubscriberIntegrationTestBase() {
    private val nsqD: KGenericContainer = KGenericContainer(DockerImageName.parse("nsqio/nsq"))
        .withCommand("/nsqd")
        .withExposedPorts(4150, 4151)

    @BeforeTest
    fun prepareContainers() {
        nsqD.start()
    }

    @AfterTest
    fun cleanup() {
        nsqD.stop()
    }

    @Test
    fun testSubscribe() {
        val sent = mutableSetOf<String>()
        val received = mutableSetOf<String>()
        val topic = "subtest#ephemeral"

        generateMessages(1, {
            sent.add(it)
            postMessages("${nsqD.containerIpAddress}:${nsqD.getMappedPort(4151)}", topic, it)
        })
        Thread.sleep(1000)
        val subscriber = DirectSubscriber("${nsqD.containerIpAddress}:${nsqD.getMappedPort(4150)}")
        subscriber.subscribe(topic, "chan", { received.add(it.data.decodeToString()) })
        val toSend = mutableSetOf<String>()
        generateMessages(300, {
            toSend.add(it)
            sent.add(it)
        })
        postMessages("${nsqD.containerIpAddress}:${nsqD.getMappedPort(4151)}", topic, *toSend.toTypedArray())

        while (received.size < sent.size) {
            Thread.sleep(100)
        }

        subscriber.stop()
        assertEquals(sent, received)
    }

}