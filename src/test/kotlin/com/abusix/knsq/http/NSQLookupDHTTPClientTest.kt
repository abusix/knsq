package com.abusix.knsq.http

import com.abusix.knsq.http.model.NsqD
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.*
import com.github.tomakehurst.wiremock.core.WireMockConfiguration
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals

class NSQLookupDHTTPClientTest {
    companion object {
        const val MOCK_HTTP_PORT = 8787
    }

    private val clsLoader = NSQLookupDHTTPClientTest::class.java.classLoader
    private val wireMockServer = WireMockServer(
        WireMockConfiguration.options()
            .port(MOCK_HTTP_PORT)
            .withRootDirectory(clsLoader.getResource("wiremock/")!!.path)
    )

    private val client = NSQLookupDHTTPClient("localhost:$MOCK_HTTP_PORT")

    @BeforeTest
    fun setup() {
        wireMockServer.start()
    }

    @AfterTest
    fun teardown() {
        wireMockServer.stop()
    }

    @Test
    fun testGetActiveTopicProducers() {
        wireMockServer.stubFor(
            get(urlEqualTo("/lookup?topic=bla"))
                .willReturn(aResponse().withStatus(200).withBodyFile("lookupd_topic_bla.json"))
        )
        val result = client.getActiveTopicProducers("bla")
        assertEquals(setOf("nsq02.example.org:4150", "nsq03.example.org:4150", "nsq01.example.org:4152"), result)
    }

    @Test
    fun testGetActiveTopicChannels() {
        wireMockServer.stubFor(
            get(urlEqualTo("/lookup?topic=bla"))
                .willReturn(aResponse().withStatus(200).withBodyFile("lookupd_topic_bla.json"))
        )
        val result = client.getActiveTopicChannels("bla")
        assertEquals(setOf("sample_channel#ephemeral"), result)
    }

    @Test
    fun testGetNodes() {
        wireMockServer.stubFor(
            get(urlEqualTo("/nodes"))
                .willReturn(aResponse().withStatus(200).withBodyFile("lookupd_nodes.json"))
        )
        val result = client.getNodes()

        val producer1 = NsqD("localhost", 4153, version = "1.2.0", httpPort = 8787, hostname = "nsqd01")
        val producer2 = NsqD("hosted.host", 4150, version = "1.2.0", httpPort = 8787, hostname = "nsqd02")
        assertEquals(listOf(producer1, producer2), result)
    }

    @Test
    fun testGetTopics() {
        wireMockServer.stubFor(
            get(urlEqualTo("/topics"))
                .willReturn(aResponse().withStatus(200).withBodyFile("lookupd_topics.json"))
        )
        val result = client.getTopics()
        assertEquals(setOf("streams.sample.bla", "another.stream", "this.is.cool#ephemeral", "andafinalone"), result)
    }

    @Test
    fun testCreateTopic() {
        wireMockServer.stubFor(
            post(urlEqualTo("/topic/create?topic=bla"))
                .willReturn(aResponse().withStatus(200))
        )
        client.createTopic("bla")
        wireMockServer.verify(postRequestedFor(urlEqualTo("/topic/create?topic=bla")))
    }

    @Test
    fun testTombstoneTopic() {
        wireMockServer.stubFor(
            post(urlEqualTo("/topic/tombstone?topic=bla&node=localhost%3A7890"))
                .willReturn(aResponse().withStatus(200))
        )
        client.tombstoneTopic("bla", "localhost:7890")
        wireMockServer.verify(postRequestedFor(urlEqualTo("/topic/tombstone?topic=bla&node=localhost%3A7890")))
    }

    @Test
    fun testDeleteTopic() {
        wireMockServer.stubFor(
            post(urlEqualTo("/topic/delete?topic=bla"))
                .willReturn(aResponse().withStatus(200))
        )
        client.deleteTopic("bla")
        wireMockServer.verify(postRequestedFor(urlEqualTo("/topic/delete?topic=bla")))
    }

    @Test
    fun testCreateChannel() {
        wireMockServer.stubFor(
            post(urlEqualTo("/channel/create?topic=bla&channel=hey"))
                .willReturn(aResponse().withStatus(200))
        )
        client.createChannel("bla", "hey")
        wireMockServer.verify(postRequestedFor(urlEqualTo("/channel/create?topic=bla&channel=hey")))
    }

    @Test
    fun testDeleteChannel() {
        wireMockServer.stubFor(
            post(urlEqualTo("/channel/delete?topic=bla&channel=hey"))
                .willReturn(aResponse().withStatus(200))
        )
        client.deleteChannel("bla", "hey")
        wireMockServer.verify(postRequestedFor(urlEqualTo("/channel/delete?topic=bla&channel=hey")))
    }
}