package com.abusix.knsq.common

import org.slf4j.Logger
import java.io.Closeable
import java.io.IOException

private val VALID_NAME_REGEX = Regex("^[.a-zA-Z0-9_-]+(?:#ephemeral)?$")

/**
 * Close this [Closeable] without throwing any kind of an [IOException], if it occurs. Such exceptions will be logged
 * on INFO level. Other Exceptions will still be thrown. This method might be useful for cleaning up connections
 * whose current state is unclear.
 */
fun Closeable.closeQuietly(logger: Logger) {
    try {
        close()
    } catch (e: IOException) {
        logger.info("Tried to close closable quietly, but IOException occurred.", e)
    }
}

/**
 * Checks whether the current string is a valid NSQ topic or channel name or not. In particular, this ensures:
 * * Not empty
 * * Maximum length of 64 characters
 * * Only alphanumerical characters, dots, underscores or hyphens
 * * A `#ephemeral` suffix is allowed and is also included in the maximum length
 */
fun String.isValidNSQTopicOrChannel() = length <= 64 && VALID_NAME_REGEX.matches(this)