package com.abusix.knsq.subscribe

import com.abusix.knsq.common.isValidNSQTopicOrChannel
import com.abusix.knsq.config.ClientConfig
import com.abusix.knsq.http.NSQLookupDHTTPClient
import com.abusix.knsq.protocol.Error
import com.abusix.knsq.protocol.Message
import com.google.common.net.HostAndPort
import com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.util.concurrent.*

/**
 * This class implements a high-level subscriber that connects to one or multiple nsqlookupd instances. Subscribed
 * topics are queried on all nodes and connections to all discovered nsqd producer nodes will be created.
 * If you want to subscribe to a nsqd node directly without using nsqlookupd, use [DirectSubscriber] instead.
 *
 * Each instance of a Subscriber creates at least one additional thread per nsqd instance for listening.
 * Additional threads might be created depending on the constructor used.
 *
 * Keep in mind that instances of this class must be cleaned up using the [stop] method. Not doing so might result
 * in memory leaks and non-terminating threads.
 */
@Suppress("UnstableApiUsage")
open class Subscriber private constructor(
    private val clientConfig: ClientConfig,
    val lookupInterval: Duration,
    val maxFlushDelay: Duration,
    private val executorsCreated: Boolean,
    internal val handlerExecutor: ExecutorService,
    internal val scheduledExecutor: ScheduledExecutorService,
    vararg lookupHosts: String
) {
    /**
     * Create a new Subscriber. Use this constructor if you want to use the embedded thread pools for handling
     * messages and scheduling tasks. Those pools will be closed later when [stop] is used.
     *
     * Additional configuration options will be available on the constructed instance.
     */
    @JvmOverloads
    constructor(
        /**
         * The hosts (and optional HTTP ports) of the nsqlookupd nodes. Example: localhost:4161.
         * The default port is 4161, if not specified.
         */
        vararg lookupHosts: String,
        /**
         * The configuration for this subscriber.
         */
        clientConfig: ClientConfig = ClientConfig(),
        /**
         * The interval for lookups. Shorter periods mean faster discovery of new nodes or topics but cause also a
         * higher amount of traffic. In addition, temporary broken connections will be resumed faster with smaller
         * lookup intervals as the resumption process is coupled to the lookup process.
         */
        lookupInterval: Duration = DEFAULT_LOOKUP_INTERVAL,
        /**
         * The maximum delay until responses to the nsq nodes are flushed. Don't set this too high, as this might
         * lead to timeouts for message acknowledgements.
         */
        maxFlushDelay: Duration = DEFAULT_MAX_FLUSH_DELAY
    ) : this(
        clientConfig, lookupInterval, maxFlushDelay, true, Executors.newFixedThreadPool(5),
        ScheduledThreadPoolExecutor(3).apply { removeOnCancelPolicy = true }, *lookupHosts
    )

    /**
     * Create a new Subscriber. Use this constructor if you want to use custom executors for thread management.
     * Those executors will not be shut down when [stop] is called and can safely be used with multiple subscribers
     * or publishers.
     *
     * Additional configuration options will be available on the constructed instance.
     */
    @JvmOverloads
    constructor(
        /**
         * The hosts (and optional HTTP ports) of the nsqlookupd nodes. Example: localhost:4161.
         * The default port is 4161, if not specified.
         */
        vararg lookupHosts: String,
        /**
         * The configuration for this subscriber.
         */
        clientConfig: ClientConfig = ClientConfig(),
        /**
         * The interval for lookups. Shorter periods mean faster discovery of new nodes or topics but cause also a
         * higher amount of traffic. In addition, temporary broken connections will be resumed faster with smaller
         * lookup intervals as the resumption process is coupled to the lookup process.
         */
        lookupInterval: Duration = DEFAULT_LOOKUP_INTERVAL,
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
    ) : this(
        clientConfig, lookupInterval, maxFlushDelay, false, handlerExecutor, scheduledExecutor, *lookupHosts
    )

    companion object {
        /**
         * The default interval for lookups.
         */
        val DEFAULT_LOOKUP_INTERVAL: Duration = Duration.ofSeconds(60)

        /**
         * The default maximum delay until responses to the nsq nodes are flushed.
         */
        val DEFAULT_MAX_FLUSH_DELAY: Duration = Duration.ofMillis(2000)

        /**
         * The default maximum amount of unflushed bytes.
         */
        const val DEFAULT_MAX_UNFLUSHED_BYTES: Long = 1000

        /**
         * The default maximum amount of attempts to deliver a message.
         */
        const val DEFAULT_MAX_MESSAGE_ATTEMPTS = Int.MAX_VALUE
        private val logger = LoggerFactory.getLogger(Subscriber::class.java)
    }

    private val lookupClients: List<NSQLookupDHTTPClient> = lookupHosts.map {
        NSQLookupDHTTPClient(it, lookupInterval.dividedBy(2), lookupInterval.dividedBy(2))
    }
    private val subscriptions = mutableListOf<Subscription>()

    @Volatile
    private var running = false

    private val lookupTask = scheduledExecutor.scheduleAtFixedRate(
        ::lookup,
        lookupInterval.toMillis(), lookupInterval.toMillis(), TimeUnit.MILLISECONDS
    )

    /**
     * The maximum amount of attempts to deliver a message. After this limit is exceeded, the message will be
     * delivered to the onFailedMessage handler that was provided with the [subscribe] method call.
     */
    var maxMessageAttempts: Int = DEFAULT_MAX_MESSAGE_ATTEMPTS
        set(value) {
            require(value >= 0)
            field = value
        }

    /**
     * The initial setting for [Subscription.maxInFlight]. This must be a positive value.
     */
    var initialMaxInFlight: Int = 200
        set(value) {
            require(value >= 0)
            field = value
        }

    /**
     * The maximum amount of unflushed bytes. If this amount is surpassed, the current write buffer to
     * nsqd will be flushed independent of the maximum flush delay.
     */
    var maxUnflushedBytes: Long = DEFAULT_MAX_UNFLUSHED_BYTES
        set(value) {
            require(value >= 0)
            field = value
        }

    /**
     * An exception listener that can be used to receive exceptions that were thrown while trying to contact the
     * nsqlookupd server. An exception being thrown here does not necessarily mean that the subscribers are no longer
     * working.
     */
    var onLookupException: ((Exception) -> Unit)? = null

    /**
     * Subscribe to a topic using a certain channel. This method returns a [Subscription] that can be used for
     * further configuration and interaction. For details about valid topics and channels,
     * see [isValidNSQTopicOrChannel].
     *
     * @param topic The topic to subscribe to.
     * @param channel The channel to use for the subscription.
     * @param onMessage A callback function for receiving messages. This callback must be threadsafe. Exceptions will
     *                  cause a message to be requeued up to [maxMessageAttempts] times. You can also manually control
     *                  the lifecycle of the [Message] object using its methods like [Message.finish].
     * @param onFailedMessage A callback function for receiving failed messages. This callback must be threadsafe.
     *                        Messages received here failed to be delivered mor than [maxMessageAttempts] times to
     *                        [onMessage] and will no longer be requeued.
     * @param onException A listener for Exceptions that happened within this [Subscriber] or its [Subscription]s.
     *                    It is guaranteed that for each error, at least [onException] or [onNSQError] will be called.
     *                    Duplicate calls for both handlers should be rare but might be possible due to race conditions.
     * @param onNSQError A listener for errors that were returned by nsqd within this [Subscriber] or
     *                   its [Subscription]s.
     *                   It is guaranteed that for each error, at least [onException] or [onNSQError] will be called.
     *                   Duplicate calls for both handlers should be rare but might be possible due to race conditions.
     * @param autoFinish Whether or not messages should be automatically marked as finished after [onMessage]
     *                   successfully concluded.
     */
    @JvmOverloads
    @Synchronized
    fun subscribe(
        topic: String, channel: String, onMessage: (Message) -> Unit,
        onFailedMessage: ((Message) -> Unit)? = null,
        onException: ((Exception) -> Unit)? = null,
        onNSQError: ((Error) -> Unit)? = null,
        autoFinish: Boolean = true
    ): Subscription {
        require(topic.isValidNSQTopicOrChannel()) { "Invalid topic" }
        require(channel.isValidNSQTopicOrChannel()) { "Invalid channel" }
        val nsqds = lookupTopic(topic, catchExceptions = false).toMutableSet()
        if (nsqds.isEmpty()) {
            // Choose a random nsqd to try to connect to for the first time.
            // This way we check authentication as soon as possible.
            val allNodes = lookupClients.map { it.getNodes() }.flatten()
            if (allNodes.isNotEmpty()) {
                allNodes.random().let {
                    nsqds.add(it.toTCPHostAndPort())
                }
            }
        }
        val sub = Subscription(clientConfig, topic, channel, this, autoFinish)
        subscriptions.add(sub)
        running = true
        sub.onMessage = onMessage
        sub.onException = onException
        sub.onFailedMessage = onFailedMessage
        sub.onNSQError = onNSQError
        sub.updateConnections(nsqds)
        return sub
    }

    @Synchronized
    private fun lookup() {
        for (sub in subscriptions) {
            if (!running) {
                return
            }
            try {
                sub.updateConnections(lookupTopic(sub.topic))
            } catch (e: Exception) {
                if (running) {
                    sub.onException?.invoke(e)
                }
                logger.debug("Exception while looking up new nsqds for topic ${sub.topic}", e)
            }
        }
    }

    protected open fun lookupTopic(topic: String, catchExceptions: Boolean = true): Set<HostAndPort> {
        val nsqds = mutableSetOf<HostAndPort>()
        for (lookup in lookupClients) {
            try {
                nsqds.addAll(lookup.getActiveTopicProducers(topic).map(HostAndPort::fromString))
            } catch (e: Exception) {
                if (!catchExceptions) {
                    throw e
                }
                logger.debug("Exception while looking up new nsqds from $lookup for topic $topic", e)
                onLookupException?.invoke(e)
            }
        }
        return nsqds
    }

    /**
     * Stops this subscriber and all of its associated connections. [maxTime] might not always be respected.
     * Synchronous cleanup tasks may need longer.
     *
     * @param maxTime The maximum time asynchronous tasks are allowed to take to finish their work.
     */
    fun stop(maxTime: Duration = Duration.ofSeconds(3)): Boolean {
        val start = Instant.now()
        var isClean = true
        try {
            running = false
            lookupTask.cancel(true)
            if (executorsCreated) {
                isClean = shutdownAndAwaitTermination(scheduledExecutor, maxTime)
                val remainder = Duration.between(start, Instant.now()).coerceIn(Duration.ofMillis(100), maxTime)
                if (!shutdownAndAwaitTermination(handlerExecutor, remainder)) {
                    isClean = false
                }
            }
        } finally {
            var exc: Exception? = null
            subscriptions.forEach {
                try {
                    it.stop()
                } catch (e: Exception) {
                    exc = e
                }
            }
            exc?.let { throw it }
        }
        return isClean
    }
}