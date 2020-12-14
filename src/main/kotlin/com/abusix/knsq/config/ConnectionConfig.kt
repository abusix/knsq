package com.abusix.knsq.config

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * The configuration object that holds the config status for a single [com.abusix.knsq.connection.Connection].
 * All members of this class are immutable.
 */
@Serializable
data class ConnectionConfig(
    /**
     * Each nsqd is configurable with a max-rdy-count. If the consumer sends a RDY count that is outside
     * of the acceptable range its connection will be forcefully closed.
     */
    @SerialName("max_rdy_count")
    val maxRdyCount: Int = 2500,
    /**
     * The nsqd version.
     */
    val version: String,
    /**
     * The maximum value for message timeout.
     *
     * @see ClientConfig.msgTimeout
     */
    @SerialName("max_msg_timeout")
    val maxMsgTimeout: Long = 30000,
    /**
     * Whether TLS is enabled for this connection or not.
     */
    @SerialName("tls_v1")
    val tls: Boolean,
    /**
     * Whether deflate compression is enabled for this connection or not.
     */
    val deflate: Boolean,
    /**
     * The deflate level. This value can be ignored if [deflate] is `false`.
     */
    @SerialName("deflate_level")
    val deflateLevel: Int,
    /**
     * The maximum deflate level supported by the server.
     */
    @SerialName("max_deflate_level")
    val maxDeflateLevel: Int,
    /**
     * Whether snappy compression is enabled for this connection or not.
     */
    val snappy: Boolean,
    /**
     * The interval for which nsqd expects and sends heartbeats, if no other messages were sent.
     */
    @SerialName("heartbeat_interval")
    var heartbeatInterval: Long = 30000,
    /**
     * Whether or not authorization is required by nsqd.
     */
    @SerialName("auth_required")
    val authRequired: Boolean,
    /**
     * The effective message timeout.
     * @see ClientConfig.msgTimeout
     */
    @SerialName("msg_timeout")
    val msgTimeout: Long = 30000
)