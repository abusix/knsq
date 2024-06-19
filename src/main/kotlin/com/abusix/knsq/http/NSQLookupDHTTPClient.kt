package com.abusix.knsq.http

import com.abusix.knsq.common.NSQHTTPResponseException
import com.abusix.knsq.http.model.LookupResponse
import com.abusix.knsq.http.model.NsqD
import com.abusix.knsq.http.model.TopicsResponse
import com.abusix.knsq.subscribe.Subscriber.Companion.DEFAULT_LOOKUP_INTERVAL
import com.google.common.net.HostAndPort
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.net.URLEncoder
import java.time.Duration

/**
 * A simple client for the nsqlookupd HTTP API.
 *
 * @param lookupd The host (and optional HTTP port) of the nsqlookupd instance. Example: localhost:4161.
 *                The default port is 4161, if not specified.
 * @param connectTimeout The timeout for connection to the HTTP socket.
 * @param readTimeout The timeout for reading from the established socket.
 */
class NSQLookupDHTTPClient(
    lookupd: String,
    connectTimeout: Duration = DEFAULT_LOOKUP_INTERVAL.dividedBy(2),
    readTimeout: Duration = DEFAULT_LOOKUP_INTERVAL.dividedBy(2)
) : AbstractHTTPClient(HostAndPort.fromString(lookupd).withDefaultPort(4161), false, connectTimeout, readTimeout) {

    /**
     * Returns a [LookupResponse] with all known producers and channels for a topic.
     */
    fun lookupTopic(topic: String): LookupResponse {
        return try {
            val con = performGET("lookup?topic=${URLEncoder.encode(topic, Charsets.UTF_8.name())}")
            json.decodeFromString(con.inputStream.use { it.readBytes().decodeToString() })
        } catch (e: NSQHTTPResponseException) {
            if (e.responseCode == 404) {
                LookupResponse()
            } else throw e
        }
    }

    /**
     * Returns the active channels for a topic.
     */
    fun getActiveTopicChannels(topic: String): Set<String> {
        return lookupTopic(topic).channels.toSet()
    }

    /**
     * Returns the broadcasted TCP addresses of all known nsqd producers for a topic.
     */
    fun getActiveTopicProducers(topic: String): Set<String> {
        return lookupTopic(topic).nsqds.map { it.toTCPHostAndPort().toString() }.toSet()
    }

    /**
     * Returns a list of all known nsqd instances (even if they have no active topics).
     */
    fun getNodes(): List<NsqD> {
        val con = performGET("nodes")
        val response: LookupResponse = json.decodeFromString(
            con.inputStream.use { it.readBytes().decodeToString() })
        return response.nsqds
    }

    /**
     * Returns a list of all known topics.
     */
    fun getTopics(): Set<String> {
        val con = performGET("topics")
        val response: TopicsResponse = json.decodeFromString(
            con.inputStream.use { it.readBytes().decodeToString() })
        return response.topics.toSet()
    }

    /**
     * Creates a topic in the registry of nsqlookupd. Keep in mind that this does not actually create the topic
     * at the nsqd instances.
     */
    fun createTopic(topic: String) {
        performPOST("topic/create?topic=${URLEncoder.encode(topic, Charsets.UTF_8.name())}")
    }

    /**
     * Tombstones a specific topic of a specific nsqd node. See
     * [Deletion and Tombstones](https://nsq.io/components/nsqlookupd.html#deletion_tombstones).
     *
     */
    fun tombstoneTopic(topic: String, node: String) {
        performPOST(
            "topic/tombstone?topic=${URLEncoder.encode(topic, Charsets.UTF_8.name())}" +
                    "&node=${URLEncoder.encode(node, Charsets.UTF_8.name())}"
        )
    }

    /**
     * Deletes an existing topic.
     */
    fun deleteTopic(topic: String) {
        performPOST("topic/delete?topic=${URLEncoder.encode(topic, Charsets.UTF_8.name())}")
    }

    /**
     * Creates a channel in the registry of nsqlookupd. Keep in mind that this does not actually create the channel
     * at the nsqd instances.
     */
    fun createChannel(topic: String, channel: String) {
        performPOST(
            "channel/create?topic=${URLEncoder.encode(topic, Charsets.UTF_8.name())}" +
                    "&channel=${URLEncoder.encode(channel, Charsets.UTF_8.name())}"
        )
    }

    /**
     * Deletes an existing channel of an existing topic.
     */
    fun deleteChannel(topic: String, channel: String) {
        performPOST(
            "channel/delete?topic=${URLEncoder.encode(topic, Charsets.UTF_8.name())}" +
                    "&channel=${URLEncoder.encode(channel, Charsets.UTF_8.name())}"
        )
    }

    /**
     * Returns the version of the nsqlookupd instance.
     */
    fun versionInfo(): String {
        val con = performGET("info")
        val jsonObject = json.parseToJsonElement(con.inputStream.use { it.readBytes().decodeToString() }).jsonObject
        return jsonObject["version"]?.jsonPrimitive?.content ?: ""
    }
}