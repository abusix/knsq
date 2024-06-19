package com.abusix.knsq.publish

import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.util.*
import java.util.concurrent.Future
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

/**
 * This class is always coupled with a [Publisher]. Publishers create [Batcher] instances for each topic.
 * Batchers are used within the asynchronous [Publisher.publishBuffered] method. They handle the delayed
 * and buffered publishing of messages.
 *
 * The public API of this class only exposes configuration options. You can adjust the behaviour for batchers within
 * a [Publisher] by accessing specific instances via [Publisher.getBatcher].
 */
class Batcher internal constructor(
    private val publisher: Publisher,
    private val executor: ScheduledExecutorService,
    private val topic: String,
    /**
     * The maximum size for a single message batch in bytes. This value can only be exceeded if a single message
     * is larger than the configured value.
     */
    var maxSize: Int = DEFAULT_MAX_BATCH_SIZE,
    /**
     * The maximum delay before a message is guaranteed to be sent. Messages might be sent sooner as soon as batches
     * are full, but never later than the configured Duration.
     */
    var maxDelay: Duration = DEFAULT_MAX_BATCH_DELAY
) {

    companion object {
        /**
         * The default maximum batch size in bytes.
         */
        const val DEFAULT_MAX_BATCH_SIZE = 16 * 1024

        /**
         * The default maximum batch delay.
         */
        val DEFAULT_MAX_BATCH_DELAY: Duration = Duration.ofMillis(300)
        private val logger = LoggerFactory.getLogger(Batcher::class.java)
    }

    private var batchDeque = LinkedList<Batch>()
    private var lastSendTask: Future<*>? = null
    private var batchArrayInitialCapacity = 10

    /**
     * Internal method used to publish a single message via this [Batcher] instance. This method does never block.
     * It only adds the message to the current [Batch] object, or creates a new one if [msg] is too large to be added.
     * The method also schedules send tasks if necessary.
     */
    internal fun publish(msg: ByteArray) = synchronized(batchDeque) {
        if (batchDeque.isEmpty() || !batchDeque.last().append(msg)) {
            batchDeque.add(Batch(msg))
            if (batchDeque.size == 1) {
                // We just initialized the first batch. We start a delayed task at the target send time.
                // It is not possible that there is another task unfinished at this point.
                lastSendTask = executor.schedule(::sendBatches, maxDelay.toMillis(), TimeUnit.MILLISECONDS)
            } else if (batchDeque.size == 2) {
                // We just added a second batch. The first one can be immediately sent.
                // There must be a scheduled task that is not finished, otherwise the batch deque would be empty.
                lastSendTask!!.cancel(false)
                lastSendTask = executor.submit(::sendBatches)
            }
        }
    }

    /**
     * Internal method that sends all [Batch] objects to the nsqd instance. If [includeHalfFull] is set to `false`, the
     * last [Batch] object in the queue is not sent if it is not scheduled to be sent based on the maximum delay.
     * Otherwise, the last [Batch] is sent as well in any case. Usually, the latter case only occurs during a shutdown
     * operation.
     *
     * Be careful where to use this method, as it is dangerous for unwanted deadlocks due to the usage of the
     * [Publisher] methods.
     */
    internal fun sendBatches(includeHalfFull: Boolean = false) {
        val toSend = LinkedList<Batch>()
        synchronized(batchDeque) {
            while (batchDeque.size > 1) {
                toSend.add(batchDeque.pop())
            }
            if (batchDeque.isNotEmpty()) {
                if (includeHalfFull || batchDeque.peek().sendAt.isBefore(Instant.now())) {
                    toSend.add(batchDeque.pop())
                } else {
                    // ensure that there is a task that handles this at the accurate time.
                    lastSendTask = executor.schedule(
                        ::sendBatches,
                        Duration.between(Instant.now(), batchDeque.peek().sendAt).toMillis(), TimeUnit.MILLISECONDS
                    )
                }
            }
        }
        toSend.forEach(Batch::send)
    }

    /**
     * Stops the scheduled send task, if there is any. A running task is not cancelled.
     */
    internal fun stop() {
        lastSendTask?.cancel(false)
    }

    /**
     * Inner class representing a single batch. Batches are always sent using the MPUB command.
     * This class also handles the size limitation for single batches.
     */
    private inner class Batch(firstMessage: ByteArray) {

        /**
         * The current size in bytes of this Batch.
         */
        var size: Int = firstMessage.size
            private set

        /**
         * The immutable [Instant] when this Batch must be sent, even if it is not full.
         */
        val sendAt: Instant = Instant.now().plus(maxDelay)
        private val content = ArrayList<ByteArray>().apply { add(firstMessage) }

        /**
         * Append a new message to this Batch. This method only appends a message, if its size is small enough to fit
         * into this Batch. The return value indicates whether or not the message was successfully added.
         */
        fun append(msg: ByteArray): Boolean {
            if (msg.size + size > maxSize) {
                return false
            }
            content.add(msg)
            size += msg.size
            return true
        }

        /**
         * Send the content of this Batch to nsq. This method also adjusts the initial size of the batch array, which
         * is mostly important for performance reasons.
         */
        fun send() {
            try {
                batchArrayInitialCapacity = (content.size * 1.2).toInt().coerceIn(10, maxSize)
                publisher.publishMultiple(topic, content)
            } catch (e: Exception) {
                publisher.onException?.invoke(e) ?: logger.warn("Exception while sending batch to $topic", e)
            }
        }
    }
}