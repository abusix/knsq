package com.abusix.knsq.protocol

/**
 * An abstract implementation of a message from nsqd that indicates a normal successful flow of messages.
 * An example would be the OK confirmation.
 */
data class Response(
    /** The message of the response. */
    val msg: String
) : Frame(FrameType.RESPONSE, msg.length + 4)