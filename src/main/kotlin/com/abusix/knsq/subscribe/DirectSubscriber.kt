package com.abusix.knsq.subscribe

import com.abusix.knsq.config.ClientConfig
import com.google.common.net.HostAndPort
import java.time.Duration
import java.util.concurrent.ExecutorService
import java.util.concurrent.ScheduledExecutorService

/**
 * This class implements a low-level subscriber that connects to a set of predefined nsqd instances without using
 * nsqlookupd. Use this if you don't have access to nsqlookupd or if your nsq cluster is set up without nsqlookupd.
 * If you want to use nsqlookupd, please use [Subscriber] instead.
 *
 * Each instance of a DirectSubscriber creates at least one additional thread per nsqd instance for listening.
 * Additional threads might be created depending on the constructor used.
 *
 * Keep in mind that instances of this class must be cleaned up using the [stop] method. Not doing so might result
 * in memory leaks and non-terminating threads.
 */
@Suppress("UnstableApiUsage")
class DirectSubscriber : Subscriber {
    /**
     * Create a new DirectSubscriber. Use this constructor if you want to use the embedded thread pools for handling
     * messages and scheduling tasks. Those pools will be closed later when [stop] is used.
     *
     * Additional configuration options will be available on the constructed instance.
     */
    @JvmOverloads
    constructor(
        /**
         * The hosts (with optional TCP ports) of the nsqd nodes. Example: localhost:4150.
         * The default port is 4150, if not specified.
         */
        vararg nsqdHosts: String,
        /**
         * The configuration for this subscriber.
         */
        clientConfig: ClientConfig = ClientConfig(),
        /**
         * The interval during which the live status of the connection is checked. Keep in mind: If this value is
         * higher, there is also a higher risk of longer temporary outages. For ephemeral topics or channels,
         * data might be lost during such an outage.
         */
        aliveCheckInterval: Duration = DEFAULT_ALIVE_CHECK_INTERVAL,
        /**
         * The maximum delay until responses to the nsq nodes are flushed. Don't set this too high, as this might
         * lead to timeouts for message acknowledgements.
         */
        maxFlushDelay: Duration = DEFAULT_MAX_FLUSH_DELAY
    ) : super(
        clientConfig = clientConfig, lookupInterval = aliveCheckInterval, maxFlushDelay = maxFlushDelay
    ) {
        nsqds = nsqdHosts.map { HostAndPort.fromString(it).withDefaultPort(4150) }.toSet()
    }

    /**
     * Create a new DirectSubscriber. Use this constructor if you want to use custom executors for thread management.
     * Those executors will not be shut down when [stop] is called and can safely be used with multiple subscribers
     * or publishers.
     *
     * Additional configuration options will be available on the constructed instance.
     */
    @JvmOverloads
    constructor(
        /**
         * The hosts (with optional TCP ports) of the nsqd nodes. Example: localhost:4151.
         * The default port is 4151, if not specified.
         */
        vararg nsqdHosts: String,
        /**
         * The configuration for this subscriber.
         */
        clientConfig: ClientConfig = ClientConfig(),
        /**
         * The interval during which the live status of the connection is checked. Keep in mind: If this value is
         * higher, there is also a higher risk of longer temporary outages. For ephemeral topics or channels,
         * data might be lost during such an outage.
         */
        aliveCheckInterval: Duration = DEFAULT_ALIVE_CHECK_INTERVAL,
        /**
         * The maximum delay until responses to the nsq nodes are flushed. Don't set this too high, as this might
         * lead to timeouts for message acknowledgements.
         */
        maxFlushDelay: Duration = DEFAULT_MAX_FLUSH_DELAY,
        /**
         * The executor for handling messages. The size of this executor should be equal to or larger than the
         * [initialMaxInFlight] setting. Otherwise the internal receiver loop thread could be blocked which might
         * cause unexpected side effects.
         */
        handlerExecutor: ExecutorService,
        /**
         * The executor for scheduled tasks. This is mainly used for heartbeat messages and can be rather small.
         */
        scheduledExecutor: ScheduledExecutorService
    ) : super(
        clientConfig = clientConfig, lookupInterval = aliveCheckInterval, maxFlushDelay = maxFlushDelay,
        handlerExecutor = handlerExecutor, scheduledExecutor = scheduledExecutor
    ) {
        nsqds = nsqdHosts.map { HostAndPort.fromString(it).withDefaultPort(4150) }.toSet()
    }

    companion object {
        /**
         * The default interval during which the live status of the connection is checked.
         */
        val DEFAULT_ALIVE_CHECK_INTERVAL: Duration = Duration.ofSeconds(5)
    }

    private val nsqds: Set<HostAndPort>

    override fun lookupTopic(topic: String, catchExceptions: Boolean) = nsqds
}