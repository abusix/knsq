package com.abusix.knsq.http

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.*
import com.github.tomakehurst.wiremock.core.WireMockConfiguration
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test

class NSQDHTTPClientTest {
    companion object {
        const val MOCK_HTTP_PORT = 8787
    }

    private val wireMockServer = WireMockServer(
        WireMockConfiguration.options()
            .port(MOCK_HTTP_PORT)
            .withRootDirectory(NSQLookupDHTTPClientTest::class.java.getResource("/").path)
    )

    private val client = NSQDHTTPClient("localhost:$MOCK_HTTP_PORT")

    @BeforeTest
    fun setup() {
        wireMockServer.start()
    }

    @AfterTest
    fun teardown() {
        wireMockServer.stop()
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
    fun testDeleteTopic() {
        wireMockServer.stubFor(
            post(urlEqualTo("/topic/delete?topic=bla"))
                .willReturn(aResponse().withStatus(200))
        )
        client.deleteTopic("bla")
        wireMockServer.verify(postRequestedFor(urlEqualTo("/topic/delete?topic=bla")))
    }

    @Test
    fun testEmptyTopic() {
        wireMockServer.stubFor(
            post(urlEqualTo("/topic/empty?topic=bla"))
                .willReturn(aResponse().withStatus(200))
        )
        client.emptyTopic("bla")
        wireMockServer.verify(postRequestedFor(urlEqualTo("/topic/empty?topic=bla")))
    }

    @Test
    fun testPauseTopic() {
        wireMockServer.stubFor(
            post(urlEqualTo("/topic/pause?topic=bla"))
                .willReturn(aResponse().withStatus(200))
        )
        client.pauseTopic("bla")
        wireMockServer.verify(postRequestedFor(urlEqualTo("/topic/pause?topic=bla")))
    }

    @Test
    fun testUnpauseTopic() {
        wireMockServer.stubFor(
            post(urlEqualTo("/topic/unpause?topic=bla"))
                .willReturn(aResponse().withStatus(200))
        )
        client.unpauseTopic("bla")
        wireMockServer.verify(postRequestedFor(urlEqualTo("/topic/unpause?topic=bla")))
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

    @Test
    fun testEmptyChannel() {
        wireMockServer.stubFor(
            post(urlEqualTo("/channel/empty?topic=bla&channel=hey"))
                .willReturn(aResponse().withStatus(200))
        )
        client.emptyChannel("bla", "hey")
        wireMockServer.verify(postRequestedFor(urlEqualTo("/channel/empty?topic=bla&channel=hey")))
    }

    @Test
    fun testPauseChannel() {
        wireMockServer.stubFor(
            post(urlEqualTo("/channel/pause?topic=bla&channel=hey"))
                .willReturn(aResponse().withStatus(200))
        )
        client.pauseChannel("bla", "hey")
        wireMockServer.verify(postRequestedFor(urlEqualTo("/channel/pause?topic=bla&channel=hey")))
    }

    @Test
    fun testUnpauseChannel() {
        wireMockServer.stubFor(
            post(urlEqualTo("/channel/unpause?topic=bla&channel=hey"))
                .willReturn(aResponse().withStatus(200))
        )
        client.unpauseChannel("bla", "hey")
        wireMockServer.verify(postRequestedFor(urlEqualTo("/channel/unpause?topic=bla&channel=hey")))
    }
}