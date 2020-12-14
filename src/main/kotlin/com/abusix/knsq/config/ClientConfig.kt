package com.abusix.knsq.config

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import java.lang.management.ManagementFactory
import java.util.*
import javax.net.ssl.SSLSocketFactory

/**
 * This class is used for configuring the clients for nsq. Immutable properties must be set when creating the object and
 * are sent to NSQ for feature specification or negotiation. Keep in mind that some features might require some
 * configuration on the server-side and could be not available.
 */
@Suppress("unused")
@Serializable
class ClientConfig(
    /**
     * An identifier used to disambiguate this client (i.e. something specific to the consumer)
     */
    @SerialName("client_id")
    val clientId: String = "",
    /**
     * The hostname where the client is deployed
     */
    @SerialName("hostname")
    val localHostname: String = getDefaultHostname(),
    /**
     * Boolean used to indicate that the client supports feature negotiation. If the server is capable,
     * it will send back a JSON payload of supported features and metadata.
     */
    @SerialName("feature_negotiation")
    val featureNegotiation: Boolean = true,
    /**
     * Milliseconds between heartbeats.
     *
     * Valid range: `1000 <= heartbeat_interval <= configured_max` (`-1` disables heartbeats)
     */
    @SerialName("heartbeat_interval")
    val heartbeatInterval: Int = 30000,
    /**
     * Enable TLS for this connection
     */
    @SerialName("tls_v1")
    val tls: Boolean = false,
    /**
     * Enable snappy compression for this connection. A client cannot enable both [snappy] and [deflate].
     */
    val snappy: Boolean = false,
    /**
     * Enable deflate compression for this connection. A client cannot enable both [snappy] and [deflate].
     */
    val deflate: Boolean = false,
    /**
     * Configure the deflate compression level for this connection.
     *
     * Valid range: `1 <= deflate_level <= configured_max`
     *
     * Higher values mean better compression but more CPU usage for nsqd.
     */
    @SerialName("deflate_level")
    val deflateLevel: Int = 6,
    /**
     * A string identifying the agent for this client in the spirit of HTTP.
     */
    @SerialName("user_agent")
    val userAgent: String = "knsq/" + retrieveProjectVersion(),
    /**
     * Configure the server-side message timeout in milliseconds for messages delivered to this client.
     */
    @SerialName("msg_timeout")
    val msgTimeout: Int = 60000,
    /**
     * The sample rate for incoming data to deliver a percentage of all messages received to this connection.
     * This only applies to subscribing connections. The valid range is between 0 and 99, where 0 means that all
     * data is sent (this is the default). 1 means that 1% of the data is sent.
     */
    @SerialName("sample_rate")
    val sampleRate: Int = 0,
    /**
     * The secret used for authorization, if the server requires it. This value will be ignored if the server
     * does not require authorization.
     */
    @Transient
    var authSecret: ByteArray = byteArrayOf(),
    /**
     * The read timeout for connection sockets and for awaiting responses from nsq.
     */
    @Transient
    var readTimeout: Int = 30000,
    /**
     * The timeout for establishing a connection.
     */
    @Transient
    var connectTimeout: Int = 30000,
    /**
     * Using this property, you can use custom SSL configurations, e.g. self signed certificates.
     */
    @Transient
    var sslSocketFactory: SSLSocketFactory = SSLSocketFactory.getDefault() as SSLSocketFactory
)

private fun getDefaultHostname(): String {
    val pidHost = ManagementFactory.getRuntimeMXBean().name
    val pos = pidHost.indexOf('@')
    if (pos > 0) {
        return pidHost.substring(pos + 1)
    }
    return ""
}

private fun retrieveProjectVersion(): String {
    val props = Properties()
    props.load(ClientConfig::class.java.classLoader.getResourceAsStream("knsq.properties"))
    return props["knsq.version"].toString()
}