package com.abusix.knsq.protocol

import com.abusix.knsq.connection.Connection
import com.abusix.knsq.connection.SubConnection
import java.time.Duration
import java.time.Instant

/**
 * An abstract implementation that represents a message sent from nsqd to the client.
 */
class Message internal constructor(
    rawTimestamp: Long,
    /**
     * The amount of attempts that already happened for this message. The current attempt is included.
     */
    val attempts: Int,
    /**
     * A unique identifier of the message mainly used for sending an acknowledgement.
     */
    val id: String,
    /**
     * The data payload of the message.
     */
    val data: ByteArray,
    private val connection: Connection
) : Frame(FrameType.MESSAGE, data.size + 30) {

    companion object {
        private const val NANOS_PER_SECOND = 1000_000_000L
    }

    /**
     * The timestamp of this message. Keep in mind that this is an implementation detail of nsqd and has no guarantees.
     */
    val timestamp: Instant = Instant.ofEpochSecond(rawTimestamp / NANOS_PER_SECOND, rawTimestamp % NANOS_PER_SECOND)

    /**
     * Whether or not this message was already confirmed as finished via a message to nsqd or not.
     * If this is true, [finish], [requeue] and [touch] will be without effect.
     */
    var isFinished = false
        private set

    /**
     * Whether or not this message was already requeued at nsqd. Keep in mind that this does not take the server-side
     * timeout into effect.
     * If this is true, [finish], [requeue] and [touch] will be without effect.
     */
    var isRequeued = false
        private set

    /**
     * The topic this message was sent to
     */
    val topic: String
        get() = run {
            if (connection !is SubConnection) {
                throw IllegalStateException("Connection related to message is no SubConnection. Implementation error?")
            }
            connection.topic
        }

    /**
     * Finish this message for good. This means that nsqd won't send this message again and drops it if no other clients
     * have this message still pending.
     */
    fun finish() {
        if (connection !is SubConnection) {
            throw IllegalStateException("Connection related to message is no SubConnection. Implementation error?")
        }
        if (isFinished || isRequeued) {
            return
        }
        connection.finish(id)
        isFinished = true
    }

    /**
     * Requeue this message at nsqd. The message will usually will be appended to the end of the queue there. However,
     * this is no guaranteed behaviour of nsqd.
     */
    fun requeue(delay: Duration = Duration.ZERO) {
        if (connection !is SubConnection) {
            throw IllegalStateException("Connection related to message is no SubConnection. Implementation error?")
        }
        if (isFinished || isRequeued) {
            return
        }
        connection.requeue(id, delay)
        isRequeued = true
    }

    /**
     * Touch this message to reset the processing timeout. Use this if your client implementation performs manual
     * [finish] calls that might happen after a while.
     */
    fun touch() {
        if (connection !is SubConnection) {
            throw IllegalStateException("Connection related to message is no SubConnection. Implementation error?")
        }
        if (isFinished || isRequeued) {
            return
        }
        connection.touch(id)
    }
}