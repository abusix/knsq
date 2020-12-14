package com.abusix.knsq.protocol

/**
 * A subtype of [Frame] for error-messages returned by nsqd. Most of those errors terminate the connections.
 * You can check this via the [ErrorType.terminateConnection] property of [errorType].
 */
data class Error(
    /**
     * The raw content of the error returned by nsqd. This usually includes the string representation
     * of [errorType] in the beginning.
     */
    val rawData: String
) : Frame(FrameType.ERROR, rawData.length + 4) {
    /**
     * The type of this error.
     */
    val errorType = try {
        ErrorType.valueOf(rawData.split(' ')[0])
    } catch (e: IllegalArgumentException) {
        ErrorType.E_INVALID
    }
}

enum class ErrorType(
    /**
     * A boolean indicating whether or not an [Error] with this type terminates the connection or not.
     */
    val terminateConnection: Boolean
) {
    /**
     * A generic error type without any more hints.
     */
    E_INVALID(true),

    /**
     * This error might be returned during multiple occasions. It can be returned for IDENTIFY, AUTH or MPUB messages.
     * It is caused for payloads that do not meet certain requirements. For IDENTIFY and AUTH, this is usually a bug in
     * the library and should be reported. For MPUB, this error can occur if the payload is larger than the maximum
     * payload size specified in the nsqd config. You should consider lowering the maximum batch size for
     * [com.abusix.knsq.publish.Batcher] instances in [com.abusix.knsq.publish.Publisher].
     */
    E_BAD_BODY(true),

    /**
     * This error indicates that the topic sent to nsqd is not valid. This should never happen using this library and
     * is considered a bug, as the library should already ensure that the topics are valid.
     */
    E_BAD_TOPIC(true),

    /**
     * This error indicates that the channel sent to nsqd is not valid. This should never happen using this library and
     * is considered a bug, as the library should already ensure that the channels are valid.
     */
    E_BAD_CHANNEL(true),

    /**
     * This error is returned by nsqd if the message in the payload of a publishing operation does not meet the
     * requirements of the server. This might be caused by too big payloads being sent to nsqd. You should consider
     * adding a limit to the payload size or increasing it in the nsqd config.
     */
    E_BAD_MESSAGE(true),

    /**
     * This error may happen if a error condition is met after validating the input on the nsqd side. This is usually a
     * temporary error and can be caused by topics being added, deleted or cleared.
     */
    E_PUB_FAILED(true),

    /**
     * This error may happen if a error condition is met after validating the input on the nsqd side. This is usually a
     * temporary error and can be caused by topics being added, deleted or cleared.
     */
    E_MPUB_FAILED(true),

    /**
     * This error may happen if a error condition is met after validating the input on the nsqd side. This is usually a
     * temporary error and can be caused by topics being added, deleted or cleared.
     */
    E_DPUB_FAILED(true),

    /**
     * This error may happen if a error condition is met after validating the input on the nsqd side. This can
     * happen in particular for messages that are no longer queued on the server side.
     */
    E_FIN_FAILED(false),

    /**
     * This error may happen if a error condition is met after validating the input on the nsqd side. This can
     * happen in particular for messages that are no longer queued on the server side.
     */
    E_REQ_FAILED(false),

    /**
     * This error may happen if a error condition is met after validating the input on the nsqd side. This can
     * happen in particular for messages that are no longer queued on the server side.
     */
    E_TOUCH_FAILED(false),

    /**
     * This error indicates that the authorization of the client failed on the server side. This might be related
     * to connection issues to the authorization server. Depending on the authorization server implementation, this
     * might also indicate that the given auth secret in the [com.abusix.knsq.config.ClientConfig] is not known
     * on the server or the server denied authentication with the current connection
     * properties (i.e. TLS status and IP).
     */
    E_AUTH_FAILED(true),

    /**
     * This error happens if something breaks on the nsqd side while performing the authorization. This might be
     * caused by bugs in nsqd, the authorization server or network issues.
     */
    E_AUTH_ERROR(true),

    /**
     * This error is sent by nsqd if the client attempts an authentication, but the server does not support it. Using
     * knsq, this should never happen as authorization requests are only sent if the server supports it. It is safe
     * to expect that this error is never thrown.
     */
    E_AUTH_DISABLED(true),

    /**
     * This error indicates that the client related to the authorization secret set in the
     * [com.abusix.knsq.config.ClientConfig] is not allowed to do the operation it tried to do.
     */
    E_UNAUTHORIZED(true);
}