package com.abusix.knsq.http.model

import com.google.common.net.HostAndPort
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * A simple data class for the structure that is returned by several HTTP API call endpoints of nsqlookupd.
 */
@Serializable
data class LookupResponse(
    /**
     * The list of nsqd nodes that are active in the current requested context.
     * This depends on the specified parameters and might be empty.
     */
    @SerialName("producers")
    val nsqds: List<NsqD> = listOf(),
    /**
     * The list of channels that are active in the current requested context.
     * This depends on the specified parameters and the called API method and might be empty.
     */
    val channels: List<String> = listOf()
)

/**
 * A data class that represents a single nsqd producer node returned by nsqlookupd.
 */
@Serializable
data class NsqD(
    /**
     * The broadcast address of the nsqd node. This is usually the hostname or IP under which the node is
     * reachable from within the cluster and/or outside. This can be used together with [tcpPort] as an input
     * for [com.abusix.knsq.subscribe.DirectSubscriber] and [com.abusix.knsq.publish.Publisher].
     */
    @SerialName("broadcast_address")
    val broadcastAddress: String,
    /**
     * The TCP port of the nsqd node. This is the port that can be used with the TCP nsqd protocol. This value can
     * be used together with [broadcastAddress] as an input for [com.abusix.knsq.subscribe.DirectSubscriber]
     * and [com.abusix.knsq.publish.Publisher].
     */
    @SerialName("tcp_port")
    val tcpPort: Int,
    /**
     * The list of currently known topics on this nsqd node. Keep in mind that this list may not be the same
     * for all nodes within the cluster.
     */
    val topics: List<String> = listOf(),
    /**
     * A list of boolean flags that indicate whether or not a given topic in [topics] is tombstoned. This means that
     * the topic is no longer visible for consumers and will be deleted soon. For more information, see
     * [here](https://nsq.io/components/nsqlookupd.html).
     * This list might be empty depending of the API method. In this case, it is safe to assume that all topics
     * in [topics] are not tombstoned.
     */
    val tombstones: List<Boolean> = listOf(),
    /**
     * The version of the nsqd node.
     */
    val version: String,
    /**
     * The HTTP port of the nsqd node. Use this for the HTTP API that can be called using
     * [com.abusix.knsq.http.NSQDHTTPClient].
     */
    @SerialName("http_port")
    val httpPort: Int,
    /**
     * The hostname of the nsqd server. This might not be reachable from the outside. For communication, use
     * [broadcastAddress] instead.
     */
    val hostname: String
) {
    /**
     * Returns [broadcastAddress] and [tcpPort] merged together into a guava [HostAndPort] instance.
     */
    fun toTCPHostAndPort(): HostAndPort = HostAndPort.fromParts(broadcastAddress, tcpPort)
}