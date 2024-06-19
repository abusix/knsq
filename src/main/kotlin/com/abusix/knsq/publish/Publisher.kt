package com.abusix.knsq.publish

import com.abusix.knsq.common.KNSQException
import com.abusix.knsq.common.isValidNSQTopicOrChannel
import com.abusix.knsq.config.ClientConfig
import com.abusix.knsq.connection.PubConnection
import com.google.common.net.HostAndPort
import com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination
import java.io.IOException
import java.time.Duration
import java.time.Instant
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService

/**
 * A high-level implementation of an NSQ publisher. Use this class to send messages to a nsqd instance. The topic for
 * each message can vary. A Publisher can NOT use nsqlookupd to discover nsqd instances.
 *
 * Each instance of a Publisher creates at least one additional thread, which is listening to nsqd. Additional threads
 * might be created depending on the constructor used.
 *
 * Keep in mind that instances of this class must be cleaned up using the [stop] method. Not doing so might result
 * in memory leaks and non-terminating threads.
 */
class Publisher private constructor(
    nsqd: String,
    private val clientConfig: ClientConfig,
    private val localExecutor: Boolean,
    private val executor: ScheduledExecutorService
) {
    /**
     * Create a Publisher using internal [ScheduledExecutorService]s. For applications with just a few Publishers, this
     * should be fine to use. For high-throughput applications with several Publisher instances, an optimization can
     * be achieved by using the other constructor.
     *
     * The internal [ScheduledExecutorService] will create one single thread, which will be alive as long this Publisher
     * instance is alive.
     *
     * @param nsqd The TCP endpoint of the target nsqd instance. The format must be similar to `localhost:4150`. The
     *             port is not required and defaults to `4150`.
     * @param clientConfig The configuration for this Publisher to use. Additional configuration can be done to the
     *                     [Batcher] instances of this publishers per-topic using the [getBatcher] method.
     */
    @JvmOverloads
    constructor(nsqd: String, clientConfig: ClientConfig = ClientConfig())
            : this(nsqd, clientConfig, true, Executors.newSingleThreadScheduledExecutor())

    /**
     * Create a Publisher with a specified [ScheduledExecutorService]. Publishers usually don't require many concurrent
     * threads to be active. The executor is only used to manage heartbeats and regular [Batcher] pushes.
     *
     * @param nsqd The TCP endpoint of the target nsqd instance. The format must be similar to `localhost:4150`. The
     *             port is not required and defaults to `4150`.
     * @param clientConfig The configuration for this Publisher to use. Additional configuration can be done to the
     *                     [Batcher] instances of this publishers per-topic using the [getBatcher] method.
     * @param executor The executor to use for background tasks.
     */
    @JvmOverloads
    constructor(nsqd: String, executor: ScheduledExecutorService, clientConfig: ClientConfig = ClientConfig())
            : this(nsqd, clientConfig, false, executor)


    private val batchers = mutableMapOf<String, Batcher>()

    /**
     * A listener that can be used to obtain asynchronous exceptions. It is guaranteed that one of those two events
     * will happen for each error or exception in any backend class:
     *
     * - [onException] is called
     * - The exception is thrown by the called method (for synchronous code only)
     */
    var onException: ((Exception) -> Unit)? = null

    private var connectionInitialized = false
    private val connection by lazy {
        PubConnection(clientConfig, HostAndPort.fromString(nsqd).withDefaultPort(4150), executor).apply {
            connectionInitialized = true
            connect()
            onException = { e -> this@Publisher.onException?.invoke(e) }
            // onNSQError can be safely ignored, as the publisher requires acknowledgements for every dataflow to nsqd.
        }
    }

    /**
     * Obtain the [Batcher] for a certain topic. For public API access, this is only useful to configure the batchers
     * per topic.
     *
     * @param topic The topic of the batcher. There is exactly one batcher per topic per Publisher instance.
     */
    fun getBatcher(topic: String): Batcher {
        if (topic !in batchers) {
            synchronized(batchers) {
                // do it again, but synchronized. This way, synchronization is never done if the key already exists.
                if (topic !in batchers) {
                    val batcher = Batcher(this, executor, topic)
                    batchers[topic] = batcher
                    return batcher
                }
            }
        }
        return batchers[topic]!!
    }

    /**
     * Trigger the connection establishing routine, if the connection is not already established.
     * This method is thread-safe and can be called multiple times without side-effects.
     *
     * This is only a convenience method to actually establish the connection at the time you want it. It is safe
     * to directly call publishing methods without calling [connect] first.
     */
    @Throws(KNSQException::class, IOException::class)
    @Synchronized
    fun connect() {
        // this is enough. if we are not connected yet, this will cause a connect call. (and might throw exceptions)
        connection
    }

    /**
     * Publishes a single message synchronously to nsqd. This method might throw Exceptions that occur while sending
     * the message to nsqd.
     *
     * @param topic The destination topic for the message. This must only consist of alphanumerical characters,
     *              dots and hyphens and have a maximum length of 64 characters. See also [isValidNSQTopicOrChannel].
     * @param data The data as byte array. This must not be empty.
     *             Keep in mind that nsqd might enforce maximum data sizes.
     */
    @Throws(KNSQException::class, IOException::class)
    fun publish(topic: String, data: ByteArray) {
        require(data.isNotEmpty()) { "Can't publish empty data" }
        require(topic.isValidNSQTopicOrChannel()) { "Invalid topic" }
        connection.publish(topic, data)
    }


    /**
     * Publishes a single deferred message synchronously to nsqd. This method might throw Exceptions that occur while
     * sending the message to nsqd.
     *
     * @param topic The destination topic for the message. This must only consist of alphanumerical characters,
     *              dots and hyphens and have a maximum length of 64 characters. See also [isValidNSQTopicOrChannel].
     * @param data The data as byte array. This must not be empty.
     *             Keep in mind that nsqd might enforce maximum data sizes.
     * @param delay The delay of the message. This must be non-negative and will be handled by nsqd directly.
     */
    @Throws(KNSQException::class, IOException::class)
    fun publishDeferred(topic: String, data: ByteArray, delay: Duration) {
        require(data.isNotEmpty()) { "Can't publish empty data" }
        require(topic.isValidNSQTopicOrChannel()) { "Invalid topic" }
        require(!delay.isNegative) { "Negative delay not allowed" }
        connection.publishDeferred(topic, data, delay)
    }

    /**
     * Publishes a multiple messages synchronously to nsqd. This method might throw Exceptions that occur while sending
     * the messages to nsqd.
     *
     * @param topic The destination topic for the messages. This must only consist of alphanumerical characters,
     *              dots and hyphens and have a maximum length of 64 characters. See also [isValidNSQTopicOrChannel].
     * @param dataList A list of data entries. There must be at least one entry and all entries must not be empty.
     *                 Keep in mind that nsqd might enforce maximum data sizes.
     */
    @Throws(KNSQException::class, IOException::class)
    fun publishMultiple(topic: String, dataList: List<ByteArray>) {
        require(dataList.isNotEmpty() && dataList.all { it.isNotEmpty() }) { "Can't publish empty data" }
        require(topic.isValidNSQTopicOrChannel()) { "Invalid topic" }
        connection.publishMultiple(topic, dataList)
    }

    /**
     * Publishes a single message asynchronously and buffered with other messages to nsqd. The buffer size can be
     * configured in the respective [Batcher] instances obtained by [getBatcher]. Single batches may only exceed the
     * maximum size if a particular single message is already bigger. Keep in mind that nsqd might enforce maximum
     * data sizes. Batches created via this method are always sent using the MPUB nsq command.
     *
     * @param topic The destination topic for the message. This must only consist of alphanumerical characters,
     *              dots and hyphens and have a maximum length of 64 characters. See also [isValidNSQTopicOrChannel].
     * @param data The data as byte array. This must not be empty.
     *             Keep in mind that nsqd might enforce maximum data sizes.
     */
    fun publishBuffered(topic: String, data: ByteArray) {
        require(data.isNotEmpty()) { "Can't publish empty data" }
        require(topic.isValidNSQTopicOrChannel()) { "Invalid topic" }
        getBatcher(topic).publish(data)
    }

    /**
     * Checks whether or not this Publisher is still alive. A fresh publisher that never connected to nsqd is always
     * considered to be alive.
     */
    fun isAlive() = !connectionInitialized || connection.isRunning

    /**
     * Stops this publisher and its associated connection. [maxTime] might not always be respected.
     * Synchronous cleanup tasks may need longer.
     *
     * @param maxTime The maximum time asynchronous tasks are allowed to take to finish their work.
     */
    fun stop(maxTime: Duration = Duration.ofSeconds(3)): Boolean {
        val start = Instant.now()
        synchronized(batchers) {
            batchers.values.forEach(Batcher::stop)
            batchers.values.forEach(Batcher::sendBatches)
        }
        if (connectionInitialized) {
            connection.stop()
        }
        if (localExecutor && !executor.isShutdown) {
            val remainder = Duration.between(start, Instant.now()).coerceIn(Duration.ofMillis(100), maxTime)
            return shutdownAndAwaitTermination(executor, remainder)
        }
        return true
    }
}