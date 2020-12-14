package com.abusix.knsq.subscribe

import com.abusix.knsq.http.NSQLookupDHTTPClient
import com.abusix.knsq.http.model.NsqD
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.core.WireMockConfiguration
import com.google.common.net.HostAndPort
import io.mockk.*
import java.util.concurrent.ScheduledExecutorService
import kotlin.reflect.full.functions
import kotlin.reflect.full.memberProperties
import kotlin.reflect.jvm.isAccessible
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

@Suppress("UnstableApiUsage")
class SubscriberTest {
    @AfterTest
    fun cleanup() {
        unmockkAll()
    }

    @Test
    fun testSubscribeInvalidTopic() {
        assertFailsWith<IllegalArgumentException> { Subscriber().subscribe("@bla", "hello", {}) }
        assertFailsWith<IllegalArgumentException> { Subscriber().subscribe("", "hello", {}) }
        assertFailsWith<IllegalArgumentException> { Subscriber().subscribe("a".repeat(65), "hello", {}) }
    }

    @Test
    fun testSubscribeInvalidChannel() {
        assertFailsWith<IllegalArgumentException> { Subscriber().subscribe("hello", "@bla", {}) }
        assertFailsWith<IllegalArgumentException> { Subscriber().subscribe("hello", "", {}) }
        assertFailsWith<IllegalArgumentException> { Subscriber().subscribe("hello", "a".repeat(65), {}) }
    }

    @Test
    fun testOnMessageAssignment() {
        var called = false
        val sub = Subscriber().subscribe("hi", "hi", { called = true })
        sub.onMessage?.invoke(mockk())
        assert(called)
    }

    @Test
    fun testOnExceptionAssignment() {
        var called = false
        val sub = Subscriber().subscribe("hi", "hi", {}, onException = { called = true })
        sub.onException?.invoke(mockk())
        assert(called)
    }

    @Test
    fun testOnFailedMessageAssignment() {
        var called = false
        val sub = Subscriber().subscribe("hi", "hi", {}, onFailedMessage = { called = true })
        sub.onFailedMessage?.invoke(mockk())
        assert(called)
    }

    @Test
    fun testOnNSQErrorAssignment() {
        var called = false
        val sub = Subscriber().subscribe("hi", "hi", {}, onNSQError = { called = true })
        sub.onNSQError?.invoke(mockk())
        assert(called)
    }

    @Test
    fun testEmptyNsqdListButNodesAvailable() {
        mockkConstructor(NSQLookupDHTTPClient::class)
        mockkConstructor(Subscription::class)
        every {
            anyConstructed<NSQLookupDHTTPClient>().getNodes()
        } returns listOf(NsqD("ADDR", 10, version = "", httpPort = 15, hostname = ""))
        every {
            anyConstructed<NSQLookupDHTTPClient>().getActiveTopicProducers(any())
        } returns setOf()
        val slot = slot<Set<HostAndPort>>()
        every { anyConstructed<Subscription>().updateConnections(capture(slot)) } returns Unit
        Subscriber("AAA").subscribe("hi", "hi", {})
        assert(slot.isCaptured)
        assertEquals(setOf(HostAndPort.fromParts("ADDR", 10)), slot.captured)
    }

    @Test
    fun testEmptyNsqdListAndNoNodes() {
        mockkConstructor(NSQLookupDHTTPClient::class)
        mockkConstructor(Subscription::class)
        every {
            anyConstructed<NSQLookupDHTTPClient>().getNodes()
        } returns listOf()
        every {
            anyConstructed<NSQLookupDHTTPClient>().getActiveTopicProducers(any())
        } returns setOf()
        val slot = slot<Set<HostAndPort>>()
        every { anyConstructed<Subscription>().updateConnections(capture(slot)) } returns Unit
        Subscriber("AAA").subscribe("hi", "hi", {})
        assert(slot.isCaptured)
        assertEquals(setOf(), slot.captured)
    }

    @Test
    fun testSubscribeWithLookupError() {
        mockkConstructor(NSQLookupDHTTPClient::class)
        mockkConstructor(Subscription::class)
        every {
            anyConstructed<NSQLookupDHTTPClient>().getNodes()
        } throws Exception()
        every {
            anyConstructed<NSQLookupDHTTPClient>().getActiveTopicProducers(any())
        } returns setOf()
        assertFailsWith<Exception> { Subscriber("AAA").subscribe("hi", "hi", {}) }
    }

    @Test
    fun testSubscribeWithLookupError2() {
        mockkConstructor(NSQLookupDHTTPClient::class)
        mockkConstructor(Subscription::class)
        every {
            anyConstructed<NSQLookupDHTTPClient>().getActiveTopicProducers(any())
        } throws Exception()
        assertFailsWith<Exception> { Subscriber("AAA").subscribe("hi", "hi", {}) }
    }

    @Test
    fun testLookupTopic() {
        val subscriber = Subscriber("localhost:45497", "localhost:45498", "localhost:45499")
        val clsLoader = SubscriberTest::class.java.classLoader
        val wireMockServer1 = WireMockServer(
            WireMockConfiguration.options()
                .port(45497)
                .withRootDirectory(clsLoader.getResource("wiremock/")!!.path)
        )
        wireMockServer1.stubFor(
            WireMock.get(WireMock.urlEqualTo("/lookup?topic=bla"))
                .willReturn(WireMock.aResponse().withStatus(200).withBodyFile("lookupd_topic_bla.json"))
        )
        val wireMockServer2 = WireMockServer(
            WireMockConfiguration.options()
                .port(45498)
                .withRootDirectory(clsLoader.getResource("wiremock/")!!.path)
        )
        wireMockServer2.stubFor(
            WireMock.get(WireMock.urlEqualTo("/lookup?topic=bla"))
                .willReturn(WireMock.aResponse().withStatus(200).withBodyFile("lookupd2_topic_bla.json"))
        )
        val wireMockServer3 = WireMockServer(
            WireMockConfiguration.options()
                .port(45499)
                .withRootDirectory(clsLoader.getResource("wiremock/")!!.path)
        )
        wireMockServer3.stubFor(
            WireMock.get(WireMock.urlEqualTo("/lookup?topic=bla"))
                .willReturn(WireMock.aResponse().withStatus(200).withBodyFile("lookupd3_topic_bla.json"))
        )

        wireMockServer1.start()
        wireMockServer2.start()
        wireMockServer3.start()

        val result = Subscriber::class.functions.first { it.name == "lookupTopic" }
            .apply { isAccessible = true }.call(subscriber, "bla", true)

        wireMockServer1.stop()
        wireMockServer2.stop()
        wireMockServer3.stop()

        assertEquals(
            listOf(
                "nsq02.example.org:4150", "nsq01.example.org:4152",
                "nsq03.example.org:4150", "nsq04.example.org:4150"
            ).map(HostAndPort::fromString).toSet(), result
        )
    }

    @Test
    fun testLookupTopicWithErrorFromOneNode() {
        val subscriber = Subscriber("localhost:45497", "localhost:45498", "localhost:45499")
        val clsLoader = SubscriberTest::class.java.classLoader
        val wireMockServer2 = WireMockServer(
            WireMockConfiguration.options()
                .port(45498)
                .withRootDirectory(clsLoader.getResource("wiremock/")!!.path)
        )
        wireMockServer2.stubFor(
            WireMock.get(WireMock.urlEqualTo("/lookup?topic=bla"))
                .willReturn(WireMock.aResponse().withStatus(200).withBodyFile("lookupd2_topic_bla.json"))
        )
        val wireMockServer3 = WireMockServer(
            WireMockConfiguration.options()
                .port(45499)
                .withRootDirectory(clsLoader.getResource("wiremock/")!!.path)
        )
        wireMockServer3.stubFor(
            WireMock.get(WireMock.urlEqualTo("/lookup?topic=bla"))
                .willReturn(WireMock.aResponse().withStatus(200).withBodyFile("lookupd3_topic_bla.json"))
        )

        wireMockServer2.start()
        wireMockServer3.start()

        val result = Subscriber::class.functions.first { it.name == "lookupTopic" }
            .apply { isAccessible = true }.call(subscriber, "bla", true)

        wireMockServer2.stop()
        wireMockServer3.stop()

        assertEquals(
            listOf(
                "nsq02.example.org:4150", "nsq03.example.org:4150",
                "nsq04.example.org:4150"
            ).map(HostAndPort::fromString).toSet(), result
        )
    }

    @Test
    fun testStop() {
        mockkConstructor(Subscription::class)
        every { anyConstructed<Subscription>().updateConnections(any()) } returns Unit
        every { anyConstructed<Subscription>().stop() } just Runs
        val subscriber = Subscriber()
        subscriber.subscribe("a", "a", {})
        assert(subscriber.stop())
        assert((Subscriber::class.memberProperties.first { it.name == "scheduledExecutor" }.apply {
            isAccessible = true
        }.getter.call(subscriber) as ScheduledExecutorService).isShutdown)
    }
}