package com.abusix.knsq.connection

import com.abusix.knsq.config.ClientConfig
import com.abusix.knsq.protocol.Message
import com.abusix.knsq.subscribe.Subscription
import com.google.common.net.HostAndPort
import org.slf4j.LoggerFactory
import java.io.IOException
import java.time.Duration
import java.time.Instant
import java.util.concurrent.TimeUnit
import kotlin.math.max

/**
 * An internal class used for connection management and its lifecycle when using a subscribing connection.
 * This class should not be used directly. Please use [com.abusix.knsq.subscribe.Subscriber] instead.
 */
@Suppress("UnstableApiUsage")
internal class SubConnection(clientConfig: ClientConfig, host: HostAndPort, private val subscription: Subscription) :
    Connection(clientConfig, host, subscription.scheduledExecutor) {
    companion object {
        private val logger = LoggerFactory.getLogger(SubConnection::class.java)
    }

    private val flushTask = executor.scheduleAtFixedRate(
        ::delayedFlush, subscription.maxFlushDelay.toMillis() / 2,
        subscription.maxFlushDelay.toMillis() / 2, TimeUnit.MILLISECONDS
    )

    private var inFlight = 0
    var maxInFlight = 0
        private set
    private var finishedCount: Long = 0
    private var requeuedCount: Long = 0

    val topic: String
        get() = subscription.topic

    var onMessage: ((Message) -> Unit)? = null
    var onFailedMessage: ((Message) -> Unit)? = null

    override fun onIncomingMessage(message: Message) {
        synchronized(this@SubConnection) { inFlight++ }
        if (message.attempts > subscription.maxMessageAttempts) {
            subscription.handlerExecutor.execute {
                try {
                    onFailedMessage?.invoke(message)
                } catch (e: Exception) {
                    logger.error("Exception while processing incoming message", e)
                    onException?.invoke(e)
                }
            }
            finish(message.id)
        } else {
            subscription.handlerExecutor.execute {
                try {
                    onMessage?.invoke(message)
                } catch (e: Exception) {
                    logger.error("Exception while processing incoming message", e)
                    onException?.invoke(e)
                }
            }
        }
    }

    @Synchronized
    fun finish(id: String) {
        if (!isRunning) {
            return
        }
        try {
            writeCommand("FIN", id)
            finishedCount++
            messageDone()
        } catch (e: IOException) {
            stop()
        }
    }

    @Synchronized
    fun requeue(id: String, delay: Duration = Duration.ZERO) {
        if (!isRunning) {
            return
        }
        try {
            writeCommand("REQ", id, delay.toMillis())
            requeuedCount++
            messageDone()
        } catch (e: IOException) {
            stop()
        }
    }

    private fun messageDone() {
        inFlight = max(inFlight - 1, 0)
        if (inFlight == 0 && !isRunning) {
            flush()
        } else {
            softFlush()
        }
    }

    @Synchronized
    fun touch(id: String) {
        if (!isRunning) {
            return
        }
        try {
            writeCommand("TOUCH", id)
            softFlush()
        } catch (e: IOException) {
            stop()
        }
    }

    @Synchronized
    private fun delayedFlush() {
        try {
            if (unflushedBytes > 0 && Instant.now().isAfter(
                    lastActionFlush.plusMillis(
                        subscription.maxFlushDelay.toMillis() / 2 + 10
                    )
                )
            ) {
                flush()
            }
        } catch (e: Exception) {
            logger.error("delayedFlush error. {}", stateDesc(), e)
            onException?.invoke(e)
            stop()
        }
    }

    private fun softFlush() {
        if (unflushedBytes >= subscription.maxUnflushedBytes) {
            flush()
        }
    }

    @Synchronized
    fun setMaxInFlight(maxInFlight: Int, isActive: Boolean = true) {
        if (this.maxInFlight == maxInFlight) {
            return
        }
        this.maxInFlight = maxInFlight
        writeCommand("RDY", maxInFlight)
        if (isActive) {
            flush()
        } else {
            output.flush() //don't update lastActionFlush time - keep connection inactive
            unflushedBytes = 0
        }
    }

    @Synchronized
    override fun connect() {
        super.connect()
        writeCommand("SUB", subscription.topic, subscription.channel)
        flushAndReadOK()
    }

    @Synchronized
    override fun stop() {
        flushTask.cancel(true)
        try {
            if (inFlight > 0) {
                setMaxInFlight(0)
            }
            flush()
        } catch (e: IOException) {
            logger.debug("IOException while stopping SubConnection", e)
        } finally {
            try {
                subscription.connectionClosed(this)
            } finally {
                super.stop()
            }
        }
    }

    override fun toString() = "SubConnection: $host ${subscription.topic}.${subscription.channel}"

    @Synchronized
    override fun stateDesc() =
        super.stateDesc() + " inFlight:$inFlight maxInFlight:$maxInFlight fin:$finishedCount req:$requeuedCount"
}