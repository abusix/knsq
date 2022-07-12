package com.abusix.knsq.subscribe

import com.abusix.knsq.common.NSQException
import com.abusix.knsq.config.ClientConfig
import com.abusix.knsq.connection.SubConnection
import com.abusix.knsq.protocol.Error
import com.abusix.knsq.protocol.Message
import com.google.common.net.HostAndPort
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.*
import java.util.concurrent.Executor
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import kotlin.math.min

/**
 * A class that represents a subscription for a specific topic and channel to multiple nsqd instances. You can
 * use this class to configure the specific behaviour of the subscription.
 */
@Suppress("UnstableApiUsage")
class Subscription internal constructor(
    private val clientConfig: ClientConfig, val topic: String, val channel: String,
    private val subscriber: Subscriber, autoFinish: Boolean
) {
    companion object {
        private val logger = LoggerFactory.getLogger(Subscription::class.java)
    }

    internal val maxMessageAttempts
        get() = subscriber.maxMessageAttempts
    internal val maxFlushDelay
        get() = subscriber.maxFlushDelay
    internal val maxUnflushedBytes
        get() = subscriber.maxUnflushedBytes

    /**
     * Whether or not this subscription is still actively running.
     */
    @Volatile
    var running: Boolean = true
        private set

    /**
     * Whether or not this subscription is currently in low flight mode, which means that the amount of nsqd producer
     * nodes is higher than [maxInFlight]. In this case, the selection of active nodes is rotated over time.
     */
    val isLowFlight: Boolean
        get() = lowFlightRotateTask != null

    /**
     * The maximum amount of messages to be in flight at the same time. This should be at least the amount of nsqd
     * nodes, as the subscription will be in low flight mode otherwise. The default value is taken from
     * [Subscriber.initialMaxInFlight].
     */
    @set:Synchronized
    var maxInFlight: Int = subscriber.initialMaxInFlight
        set(value) {
            require(value >= 0)
            field = value
            distributeMaxInFlight()
        }

    /**
     * A listener for receiving messages. This callback must be threadsafe. Exceptions will
     * cause a message to be requeued up to [maxMessageAttempts] times. You can also manually control
     * the lifecycle of the [Message] object using its methods like [Message.finish].
     *
     * Keep in mind that this method will already be set at the creation with the parameter callback provided
     * at [Subscriber.subscribe]. Multiple listeners per subscription are not possible.
     */
    var onMessage: ((Message) -> Unit)? = null

    /**
     * A callback function for receiving failed messages. This callback must be threadsafe.
     * Messages received here failed to be delivered mor than [maxMessageAttempts] times to
     * [onMessage] and will no longer be requeued.
     *
     * Keep in mind that this method might already be set at the creation with the parameter callback provided
     * at [Subscriber.subscribe]. Multiple listeners per subscription are not possible.
     */
    var onFailedMessage: ((Message) -> Unit)? = null

    /**
     * A listener for Exceptions that happened within this [Subscriber] or its [Subscription]s.
     * It is guaranteed that for each error, at least [onException] or [onNSQError] will be called.
     * Duplicate calls for both handlers should be rare but might be possible due to race conditions.
     *
     * Keep in mind that this method might already be set at the creation with the parameter callback provided
     * at [Subscriber.subscribe]. Multiple listeners per subscription are not possible.
     */
    var onException: ((Exception) -> Unit)? = null

    /**
     * A listener for errors that were returned by nsqd within this [Subscriber] or its [Subscription]s.
     * It is guaranteed that for each error, at least [onException] or [onNSQError] will be called.
     * Duplicate calls for both handlers should be rare but might be possible due to race conditions.
     *
     * Keep in mind that this method might already be set at the creation with the parameter callback provided
     * at [Subscriber.subscribe]. Multiple listeners per subscription are not possible.
     */
    var onNSQError: ((Error) -> Unit)? = null

    internal val handlerExecutor: Executor
        get() = subscriber.handlerExecutor

    internal val scheduledExecutor: ScheduledExecutorService
        get() = subscriber.scheduledExecutor
    private val connectionMap = Collections.synchronizedMap(HashMap<HostAndPort, SubConnection>())
    private var lowFlightRotateTask: ScheduledFuture<*>? = null

    /**
     * The [BackoffHandler] of this subscription. Use this to configure the options for error backoff.
     */
    val backoffHandler = BackoffHandler(
        this,
        { msg -> this@Subscription.onMessage?.invoke(msg) },
        { e -> this@Subscription.onException?.invoke(e) },
        autoFinish
    )

    @Synchronized
    internal fun updateConnections(activeHosts: Set<HostAndPort>) {
        synchronized(connectionMap) {
            val iter = connectionMap.values.iterator()
            while (iter.hasNext()) {
                try {
                    val con = iter.next()
                    if (!activeHosts.contains(con.host)) {
                        if (Instant.now().isAfter(con.lastActionFlush.plusMillis(con.config.msgTimeout * 100))) {
                            logger.debug("closing inactive connection to ${con.host} with topic $topic")
                            iter.remove()
                            con.stop()
                        }
                    }
                } catch (e: Exception) {
                    logger.debug("closing inactive connection failed", e)
                    if (running) {
                        onException?.invoke(e)
                    }
                }
            }
        }
        activeHosts.minus(connectionMap.keys).forEach {
            if (!running) {
                return
            }
            logger.debug("adding new connection to $it with topic $topic")
            val con = SubConnection(clientConfig, it, this).apply {
                onMessage = { msg -> backoffHandler.handle(msg) }
                onFailedMessage = { msg -> this@Subscription.onFailedMessage?.invoke(msg) }
                onException = { e -> this@Subscription.onException?.invoke(e) }
                onNSQError = { e -> this@Subscription.onNSQError?.invoke(e) }
            }
            try {
                con.connect()
                connectionMap[it] = con
            } catch (e: Exception) {
                try {
                    con.stop()
                } catch (ignored: Exception) {
                    // just ensure that the connection is shut down to not leak any resources!
                }
                if (e is NSQException) {
                    throw e
                }
                if (running) {
                    onException?.invoke(e)
                    logger.warn("error connecting to $it with topic $topic", e)
                }
            }
        }
        distributeMaxInFlight()
    }

    private fun distributeMaxInFlight() {
        if (!running || checkLowFlight() || connectionMap.isEmpty()) {
            return
        }
        val oldestToBeActive = Instant.now().minus(subscriber.lookupInterval.multipliedBy(2))
        var (inactiveCons, activeCons) = synchronized(connectionMap) {
            connectionMap.values.partition { it.lastActionFlush.isBefore(oldestToBeActive) }
        }
        if (activeCons.isEmpty()) {
            if (inactiveCons.isEmpty()) {
                return
            }
            activeCons = inactiveCons
            inactiveCons = emptyList()
        }
        inactiveCons.forEach { it.setMaxInFlight(1, false) }

        val remaining = maxInFlight - inactiveCons.size
        val perCon = remaining / activeCons.size
        var extra = remaining % activeCons.size
        for (con in activeCons) {
            var c = perCon
            if (extra > 0) {
                c++
                extra--
            }
            con.setMaxInFlight(min(c, con.config.maxRdyCount))
        }
    }

    private fun checkLowFlight(): Boolean {
        if (maxInFlight < connectionMap.size) {
            if (lowFlightRotateTask == null) {
                lowFlightRotateTask = scheduledExecutor.scheduleAtFixedRate(::rotateLowFlight, 10, 10, TimeUnit.SECONDS)
            }
            synchronized(connectionMap) {
                val copy = connectionMap.values.toList()
                copy.subList(0, maxInFlight).forEach {
                    it.setMaxInFlight(1)
                }
                copy.subList(maxInFlight, copy.size).forEach {
                    it.setMaxInFlight(0)
                }
            }
            return true
        }
        lowFlightRotateTask?.cancel(false)
        lowFlightRotateTask = null
        return false
    }

    @Synchronized
    private fun rotateLowFlight() {
        try {
            val (paused, ready) = synchronized(connectionMap) {
                val paused = connectionMap.values.filter { it.maxInFlight == 0 }.minByOrNull { it.lastActionFlush }
                val ready = connectionMap.values.filter { it.maxInFlight == 1 }.minByOrNull { it.lastActionFlush }
                if (paused == null || ready == null) {
                    return
                }
                Pair(paused, ready)
            }
            ready.setMaxInFlight(0)
            paused.setMaxInFlight(1)
        } catch (e: Exception) {
            logger.warn("error rotating low flight.", e)
            onException?.invoke(e)
        }
    }

    /**
     * Stops this subscription and all of its associated connections. Calling this method does not affect other
     * subscriptions within the same [Subscriber] instance.
     */
    fun stop() {
        try {
            running = false
            backoffHandler.stop()
            synchronized(backoffHandler) {
                lowFlightRotateTask?.cancel(false)
                lowFlightRotateTask = null
            }
        } finally {
            var exc: Exception? = null
            connectionMap.values.toList().forEach {
                try {
                    it.stop()
                } catch (e: Exception) {
                    exc = e
                }
            }
            exc?.let { throw it }
        }
    }

    internal fun connectionClosed(closedCon: SubConnection) {
        synchronized(connectionMap) {
            if (connectionMap[closedCon.host] == closedCon) {
                connectionMap.remove(closedCon.host)
            }
        }
    }

    override fun toString() = "subscription $topic.$channel connections: ${connectionMap.size}"
}