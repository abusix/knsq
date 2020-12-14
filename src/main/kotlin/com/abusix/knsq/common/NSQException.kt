package com.abusix.knsq.common

import com.abusix.knsq.protocol.Error

/**
 * Basic class for all Exceptions that may occur within the knsq library.
 */
open class KNSQException : Exception {
    constructor(message: String) : super(message)
    constructor(message: String, cause: Throwable) : super(message, cause)
}

/**
 * An exception that indicates that an error was returned from an NSQ endpoint.
 *
 * @property error The detailed error that NSQ returned.
 */
class NSQException(val error: Error) : KNSQException(error.rawData)

/**
 * An exception that indicates that an NSQ HTTP endpoint returned a non-expected response code.
 * Some more informational details might be included in the message.
 *
 * @property responseCode The returned response code.
 */
class NSQHTTPResponseException(val responseCode: Int, additionalMessage: String = "") :
    KNSQException("Response code from nsq HTTP API was $responseCode. $additionalMessage")

/**
 * An exception that indicates that reading from NSQ timed out. This usually happens if a confirmation message
 * is not received.
 */
class NSQTimeoutException : KNSQException("Read timed out")