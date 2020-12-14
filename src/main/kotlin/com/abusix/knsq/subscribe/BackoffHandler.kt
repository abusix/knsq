package com.abusix.knsq.subscribe

import com.abusix.knsq.protocol.Message
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CancellationException
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

/**
 * This class is always coupled with a [Subscription]. Each subscription has exactly one [BackoffHandler] instance.
 * This handler takes care of the throttling of slow subscription consumers. It does this by activating periods without
 * sending at all and periods with low traffic. The length of this low traffic periods is configurable and flexible.
 * It is increased/decreased at exponential scale.
 *
 * The public API of this class only exposes configuration options. You can adjust the delay options.
 */
class BackoffHandler internal constructor(
    private val subscription: Subscription,
    private val onMessage: (Message) -> Unit,
    private val onException: ((Exception) -> Unit)? = null
) {
    companion object {
        /**
         * The default initial length of a low traffic period after the first failed received packet.
         */
        val DEFAULT_INIT_DELAY: Duration = Duration.ofMillis(1000)

        /**
         * The default maximum delay until the full traffic volume is tried again.
         */
        val DEFAULT_MAX_DELAY: Duration = Duration.ofMillis(60000)
    }

    /**
     * The initial length of a low traffic period after the first failed received packet.
     */
    var initDelay: Duration = DEFAULT_INIT_DELAY

    /**
     * The maximum delay until the full traffic volume is tried again.
     */
    var maxDelay: Duration = DEFAULT_MAX_DELAY

    @Volatile
    private var isBackoff = false
    private var lastAttempt: Instant = Instant.EPOCH
    private var delay = Duration.ZERO
    private var failCount = 0
    private var fullSpeedMaxInFlight = 0
    private var lastResumeTask: ScheduledFuture<*>? = null


    internal fun handle(msg: Message) {
        val backoff = isBackoff
        if (backoff) {
            attemptDuringBackoff()
        }
        try {
            onMessage.invoke(msg)
            synchronized(this) {
                if (backoff) {
                    successDuringBackoff()
                }
                msg.finish()
            }
        } catch (e: Exception) {
            synchronized(this) {
                onException?.invoke(e)
                failure(msg)
            }
        }
    }

    @Synchronized
    private fun failure(msg: Message) {
        isBackoff = true
        failCount++
        if (failCount == 1) {
            delay = initDelay
            fullSpeedMaxInFlight = subscription.maxInFlight
            lastAttempt = Instant.now()
        } else {
            delay = delay.multipliedBy(2).coerceAtMost(maxDelay)
            pauseSubscription()
        }
        msg.requeue()
    }

    @Synchronized
    private fun pauseSubscription() {
        if (lastResumeTask?.isDone != false) {
            subscription.maxInFlight = 0
            lastResumeTask = subscription.scheduledExecutor.schedule({
                if (subscription.running) {
                    subscription.maxInFlight = 1
                }
            }, delay.toMillis(), TimeUnit.MILLISECONDS)
        }
    }

    @Synchronized
    private fun attemptDuringBackoff() {
        val now = Instant.now()
        val waited = Duration.between(lastAttempt, now)
        lastAttempt = if (waited < delay) {
            try {
                Thread.sleep((delay - waited).toMillis())
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
            }
            Instant.now()
        } else now
    }

    @Synchronized
    private fun successDuringBackoff() {
        delay = delay.dividedBy(2)
        if (delay < initDelay) {
            lastResumeTask?.let {
                it.cancel(false)
                if (!it.isDone) {
                    try {
                        it.get()
                    } catch (ignored: CancellationException) {
                    }
                }
            }
            isBackoff = false
            failCount = 0
            delay = Duration.ZERO
            subscription.maxInFlight = fullSpeedMaxInFlight
        } else {
            pauseSubscription()
        }
    }

    internal fun stop() {
        lastResumeTask?.cancel(true)
    }

}