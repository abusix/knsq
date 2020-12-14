package com.abusix.knsq.util

import kotlin.random.Random

open class IntegrationTestBase {
    companion object {
        val random by lazy {
            val seed = Random.nextLong()
            println("IntegrationTestBase Random seed: $seed")
            Random(seed)
        }
    }

    protected fun generateMessages(
        count: Int, onMessage: (String) -> Unit, delayChance: Double = 0.0,
        maxDelay: Long = 0
    ) {
        val allowedChars = ('A'..'Z') + ('a'..'z')
        for (i in 0 until count) {
            val len: Int = random.nextInt(1, 500)
            val msg = (1..len).map { allowedChars.random(random) }.joinToString("")
            if (random.nextDouble() < delayChance) {
                Thread.sleep(random.nextLong(maxDelay))
            }
            onMessage(msg)
        }
    }
}