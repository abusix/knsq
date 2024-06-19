package com.abusix.knsq.subscribe

import com.abusix.knsq.http.NSQLookupDHTTPClient
import com.abusix.knsq.util.KGenericContainer
import com.google.common.net.HostAndPort
import io.mockk.every
import io.mockk.mockkConstructor
import io.mockk.unmockkAll
import org.junit.jupiter.api.Timeout
import org.testcontainers.containers.Network
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals

@Timeout(60, unit = TimeUnit.SECONDS)
class SubscriberIntegrationTest : SubscriberIntegrationTestBase() {
    private val network = Network.newNetwork()
    private val nsqLookupD: KGenericContainer = KGenericContainer(DockerImageName.parse("nsqio/nsq"))
        .withCommand("/nsqlookupd")
        .withNetwork(network)
        .withNetworkAliases("nsqlookupd")
        .withExposedPorts(4161, 4160)
    private val nsqD1: KGenericContainer = KGenericContainer(DockerImageName.parse("nsqio/nsq"))
        .withCommand("/nsqd --lookupd-tcp-address=nsqlookupd:4160")
        .withNetwork(network)
        .withNetworkAliases("nsqd1")
        .withExposedPorts(4150, 4151)
    private val nsqD2: KGenericContainer = KGenericContainer(DockerImageName.parse("nsqio/nsq"))
        .withCommand("/nsqd --lookupd-tcp-address=nsqlookupd:4160")
        .withNetwork(network)
        .withNetworkAliases("nsqd2")
        .withExposedPorts(4150, 4151)

    private val hostsToReplace = mutableMapOf<String, HostAndPort>()
    private val subscriber by lazy {
        object : Subscriber(
            nsqLookupD.host + ":" + nsqLookupD.getMappedPort(4161),
            lookupInterval = Duration.ofSeconds(2)
        ) {
            override fun lookupTopic(topic: String, catchExceptions: Boolean): Set<HostAndPort> {
                return super.lookupTopic(topic, catchExceptions).map {
                    hostsToReplace.getOrDefault(it.host, it)
                }.toSet()
            }
        }
    }

    @BeforeTest
    fun prepareLookupClient() {
        // this is only because we can't set the correct outside-reachable broadcast address for nsqds.
        // We will do the mapping later when calling lookupTopic
        mockkConstructor(NSQLookupDHTTPClient::class)
        every { anyConstructed<NSQLookupDHTTPClient>().getNodes() } returns emptyList()
    }

    @BeforeTest
    fun prepareContainers() {
        nsqLookupD.start()
        nsqD1.start()
        nsqD2.start()

        hostsToReplace[nsqD1.containerId.substring(0, 12)] = HostAndPort.fromParts(
            nsqD1.host,
            nsqD1.getMappedPort(4150)
        )
        hostsToReplace[nsqD2.containerId.substring(0, 12)] = HostAndPort.fromParts(
            nsqD2.host,
            nsqD2.getMappedPort(4150)
        )
    }

    @AfterTest
    fun cleanup() {
        nsqD1.stop()
        nsqD2.stop()
        nsqLookupD.stop()
        unmockkAll()
    }

    @Test
    fun testSubscribe() {
        val sent = Collections.synchronizedSet(mutableSetOf<String>())
        val received = Collections.synchronizedSet(mutableSetOf<String>())
        val topic = "subtest#ephemeral"

        generateMessages(1, {
            sent.add(it)
            postMessages("${nsqD1.host}:${nsqD1.getMappedPort(4151)}", topic, it)
        })
        generateMessages(1, {
            sent.add(it)
            postMessages("${nsqD2.host}:${nsqD2.getMappedPort(4151)}", topic, it)
        })
        Thread.sleep(1000)
        subscriber.subscribe(topic, "chan", { received.add(it.data.decodeToString()) })
        val toSend = mutableSetOf<String>()
        generateMessages(300, {
            toSend.add(it)
            sent.add(it)
        })
        val toSendList = toSend.toList()
        postMessages(
            "${nsqD1.host}:${nsqD1.getMappedPort(4151)}", topic,
            *toSendList.subList(0, 50).toTypedArray()
        )
        postMessages(
            "${nsqD2.host}:${nsqD2.getMappedPort(4151)}", topic,
            *toSendList.subList(50, 150).toTypedArray()
        )
        postMessages(
            "${nsqD1.host}:${nsqD1.getMappedPort(4151)}", topic,
            *toSendList.subList(150, toSend.size).toTypedArray()
        )

        while (received.size < sent.size) {
            Thread.sleep(100)
        }

        subscriber.stop()
        assertEquals(sent, received)
    }

    @Test
    fun testTwoTopicsSubscribe() {
        val sent = Collections.synchronizedSet(mutableSetOf<String>())
        val received = Collections.synchronizedSet(mutableSetOf<String>())
        val topic1 = "subtest#ephemeral"
        val topic2 = "subtest2#ephemeral"

        generateMessages(1, {
            sent.add(it)
            postMessages("${nsqD1.host}:${nsqD1.getMappedPort(4151)}", topic1, it)
        })
        generateMessages(1, {
            sent.add(it)
            postMessages("${nsqD2.host}:${nsqD2.getMappedPort(4151)}", topic1, it)
        })
        Thread.sleep(1000)
        subscriber.subscribe(topic1, "chan", { received.add(it.data.decodeToString()) })
        subscriber.subscribe(topic2, "chan", { received.add(it.data.decodeToString()) })
        val toSend = mutableSetOf<String>()
        generateMessages(300, {
            toSend.add(it)
            sent.add(it)
        })
        val toSendList = toSend.toList()
        postMessages(
            "${nsqD1.host}:${nsqD1.getMappedPort(4151)}", topic1,
            *toSendList.subList(0, 50).toTypedArray()
        )
        postMessages(
            "${nsqD2.host}:${nsqD2.getMappedPort(4151)}", topic1,
            *toSendList.subList(50, 150).toTypedArray()
        )
        postMessages(
            "${nsqD1.host}:${nsqD1.getMappedPort(4151)}", topic2,
            *toSendList.subList(150, toSend.size).toTypedArray()
        )

        while (received.size < sent.size) {
            Thread.sleep(100)
        }

        subscriber.stop()
        assertEquals(sent, received)
    }

    @Test
    fun testReconnect() {
        val sent = Collections.synchronizedSet(mutableSetOf<String>())
        val received = Collections.synchronizedSet(mutableSetOf<String>())
        val topic = "subtest#ephemeral"

        generateMessages(1, {
            sent.add(it)
            postMessages("${nsqD1.host}:${nsqD1.getMappedPort(4151)}", topic, it)
        })
        Thread.sleep(1000)
        subscriber.subscribe(topic, "chan", { received.add(it.data.decodeToString()) })

        while (received.size < sent.size) {
            Thread.sleep(100)
        }

        nsqD1.stop()
        nsqD1.start()
        hostsToReplace[nsqD1.containerId.substring(0, 12)] = HostAndPort.fromParts(
            nsqD1.host,
            nsqD1.getMappedPort(4150)
        )

        val toSend = mutableSetOf<String>()
        generateMessages(100, {
            toSend.add(it)
            sent.add(it)
        })
        postMessages("${nsqD1.host}:${nsqD1.getMappedPort(4151)}", topic, *toSend.toTypedArray())

        while (received.size < sent.size) {
            Thread.sleep(100)
        }

        subscriber.stop()
        assertEquals(sent, received)
    }
}