package com.abusix.knsq.http.model

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * The response object that is returned by calling [com.abusix.knsq.http.NSQDHTTPClient.stats].
 */
@Serializable
data class StatsResponse(
    val version: String,
    val health: String,
    @SerialName("start_time")
    val startTime: Long,
    val topics: List<TopicStats>,
    val memory: MemoryStats,
    val producers: List<ClientStats>
)

@Serializable
data class TopicStats(
    @SerialName("topic_name")
    val topicName: String,
    val channels: List<ChannelStats>,
    val depth: Long,
    @SerialName("backend_depth")
    val diskDepth: Long,
    @SerialName("message_count")
    val messageCount: Long,
    @SerialName("message_bytes")
    val messageBytes: Long,
    @SerialName("e2e_processing_latency")
    val e2eProcessingLatency: E2EProcessingLatencyStats
)

@Serializable
data class E2EProcessingLatencyStats(
    val count: Int,
    val percentiles: List<E2EPercentile>?
)

@Serializable
data class E2EPercentile(
    val quantile: Float,
    val value: Long
)

@Serializable
data class ChannelStats(
    @SerialName("channel_name")
    val channelName: String,
    val depth: Long,
    @SerialName("backend_depth")
    val diskDepth: Long,
    @SerialName("in_flight_count")
    val inFlightCount: Long,
    @SerialName("deferred_count")
    val deferredCount: Long,
    @SerialName("message_count")
    val totalMessageCount: Long,
    @SerialName("requeue_count")
    val requeueCount: Long,
    @SerialName("timeout_count")
    val timeoutCount: Long,
    @SerialName("client_count")
    val clientCount: Long,
    val paused: Boolean,
    @SerialName("e2e_processing_latency")
    val e2eProcessingLatency: E2EProcessingLatencyStats,
    val clients: List<ClientStats>
)

@Serializable
data class ClientStats(
    @SerialName("client_id")
    val clientId: String,
    val hostname: String,
    val version: String,
    @SerialName("remote_address")
    val remoteAddress: String,
    val state: Int,
    @SerialName("ready_count")
    val readyCount: Int,
    @SerialName("in_flight_count")
    val inFlightCount: Long,
    @SerialName("message_count")
    val totalMessageCount: Long,
    @SerialName("requeue_count")
    val requeueCount: Long,
    @SerialName("finish_count")
    val finishCount: Long,
    @SerialName("connect_ts")
    val connectTimestamp: Long,
    @SerialName("sample_rate")
    val sampleRate: Int,
    val deflate: Boolean,
    val snappy: Boolean,
    @SerialName("user_agent")
    val userAgent: String,
    @SerialName("authed")
    val authorized: Boolean,
    @SerialName("auth_identity")
    val authIdentity: String,
    val tls: Boolean,
    @SerialName("tls_cipher_suite")
    val tlsCipherSuite: String,
    @SerialName("tls_version")
    val tlsVersion: String,
    @SerialName("tls_negotiated_protocol")
    val tlsNegotiatedProtocol: String,
    @SerialName("tls_negotiated_protocol_is_mutual")
    val tlsNegotiatedProtocolIsMutual: Boolean,
    val pubCounts: List<PublishedStats> = listOf()
)

@Serializable
data class PublishedStats(
    val topic: String,
    val count: Long
)

@Serializable
data class MemoryStats(
    @SerialName("heap_objects")
    val heapObjects: Long,
    @SerialName("heap_idle_bytes")
    val heapIdleBytes: Long,
    @SerialName("heap_in_use_bytes")
    val heapInUseBytes: Long,
    @SerialName("heap_released_bytes")
    val heapReleasedBytes: Long,
    @SerialName("gc_pause_usec_100")
    val gcPauseUsec100: Long,
    @SerialName("gc_pause_usec_99")
    val gcPauseUsec99: Long,
    @SerialName("gc_pause_usec_95")
    val gcPauseUSec95: Long,
    @SerialName("next_gc_bytes")
    val nextGcBytes: Long,
    @SerialName("gc_total_runs")
    val gcTotalRuns: Long
)