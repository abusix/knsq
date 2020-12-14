package com.abusix.knsq.http

import com.abusix.knsq.http.model.NsqD
import com.abusix.knsq.http.model.StatsResponse
import com.abusix.knsq.subscribe.Subscriber
import com.google.common.net.HostAndPort
import kotlinx.serialization.decodeFromString
import java.net.URLEncoder
import java.time.Duration

/**
 * A simple client for the nsqd HTTP API.
 *
 * @param nsqd The host (and optional HTTP port) of the nsqd instance. Example: localhost:4151.
 *             The default port is 4151, if not specified. Keep in mind that the TLS port for HTTP is different
 *             from the non-tls port.
 * @param tls Use TLS or not for the connection.
 * @param connectTimeout The timeout for connection to the HTTP socket.
 * @param readTimeout The timeout for reading from the established socket.
 */
@Suppress("UnstableApiUsage")
class NSQDHTTPClient(
    nsqd: String,
    tls: Boolean = false,
    connectTimeout: Duration = Subscriber.DEFAULT_LOOKUP_INTERVAL.dividedBy(2),
    readTimeout: Duration = Subscriber.DEFAULT_LOOKUP_INTERVAL.dividedBy(2)
) : AbstractHTTPClient(HostAndPort.fromString(nsqd).withDefaultPort(4151), tls, connectTimeout, readTimeout) {
    /**
     * Creates a topic.
     */
    fun createTopic(topic: String) {
        performPOST("topic/create?topic=${URLEncoder.encode(topic, Charsets.UTF_8.name())}")
    }

    /**
     * Delete san existing topic (and all channels).
     */
    fun deleteTopic(topic: String) {
        performPOST("topic/delete?topic=${URLEncoder.encode(topic, Charsets.UTF_8.name())}")
    }

    /**
     * Empty all the queued messages (in-memory and disk) for an existing topic.
     */
    fun emptyTopic(topic: String) {
        performPOST("topic/empty?topic=${URLEncoder.encode(topic, Charsets.UTF_8.name())}")
    }

    /**
     * Pause message flow to all channels on an existing topic (messages will queue at topic).
     */
    fun pauseTopic(topic: String) {
        performPOST("topic/pause?topic=${URLEncoder.encode(topic, Charsets.UTF_8.name())}")
    }

    /**
     * Resume message flow to channels of an existing, paused, topic.
     */
    fun unpauseTopic(topic: String) {
        performPOST("topic/unpause?topic=${URLEncoder.encode(topic, Charsets.UTF_8.name())}")
    }

    /**
     * Create a channel for an existing topic.
     */
    fun createChannel(topic: String, channel: String) {
        performPOST(
            "channel/create?topic=${URLEncoder.encode(topic, Charsets.UTF_8.name())}" +
                    "&channel=${URLEncoder.encode(channel, Charsets.UTF_8.name())}"
        )
    }

    /**
     * Delete an existing channel on an existing topic.
     */
    fun deleteChannel(topic: String, channel: String) {
        performPOST(
            "channel/delete?topic=${URLEncoder.encode(topic, Charsets.UTF_8.name())}" +
                    "&channel=${URLEncoder.encode(channel, Charsets.UTF_8.name())}"
        )
    }

    /**
     * Empty all the queued messages (in-memory and disk) for an existing channel.
     */
    fun emptyChannel(topic: String, channel: String) {
        performPOST(
            "channel/empty?topic=${URLEncoder.encode(topic, Charsets.UTF_8.name())}" +
                    "&channel=${URLEncoder.encode(channel, Charsets.UTF_8.name())}"
        )
    }

    /**
     * Pause message flow to consumers of an existing channel (messages will queue at channel).
     */
    fun pauseChannel(topic: String, channel: String) {
        performPOST(
            "channel/pause?topic=${URLEncoder.encode(topic, Charsets.UTF_8.name())}" +
                    "&channel=${URLEncoder.encode(channel, Charsets.UTF_8.name())}"
        )
    }

    /**
     * Resume message flow to consumers of an existing, paused, channel.
     */
    fun unpauseChannel(topic: String, channel: String) {
        performPOST(
            "channel/unpause?topic=${URLEncoder.encode(topic, Charsets.UTF_8.name())}" +
                    "&channel=${URLEncoder.encode(channel, Charsets.UTF_8.name())}"
        )
    }

    /**
     * Publishes a message to a topic using HTTP. The message can optionally be deferred.
     */
    @JvmOverloads
    fun publish(topic: String, message: ByteArray, deferBy: Duration? = null) {
        performPOST("pub?topic=${URLEncoder.encode(topic, Charsets.UTF_8.name())}" +
                (deferBy?.let { "&defer=${it}" } ?: ""), message)
    }

    /**
     * Returns basic information about this nsqd node.
     */
    fun info(): NsqD {
        val con = performGET("info")
        return json.decodeFromString(con.inputStream.use { it.readBytes().decodeToString() })
    }

    /**
     * Returns all internal statistics of this nsqd node.
     */
    fun stats(): StatsResponse {
        val con = performGET("stats?format=json")
        return json.decodeFromString(con.inputStream.use { it.readBytes().decodeToString() })
    }
}