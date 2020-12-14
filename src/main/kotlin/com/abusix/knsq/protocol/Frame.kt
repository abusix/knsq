package com.abusix.knsq.protocol

/**
 * An abstract representation of a parsed nsqd frame.
 */
abstract class Frame(
    /**
     * The type of this frame, which is determined by the second byte quartet of the raw frame.
     */
    val type: FrameType,
    /**
     * The size of this frame, which is determined by the first byte quartet of the raw frame.
     * This does not include the 4 bytes containing the size itself. However, the frame type byte quartet
     * is included here.
     */
    val size: Int
)

enum class FrameType {
    /**
     * A normal response which can be expected to be a non-error case.
     */
    RESPONSE,

    /**
     * A error response. For most responses, the connection is terminated afterwards. For details, see [Error].
     */
    ERROR,

    /**
     * A message sent by nsqd. This is only allowed in subscribing connections.
     */
    MESSAGE
}