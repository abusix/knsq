package com.abusix.knsq.connection

import com.abusix.knsq.common.KNSQException
import com.abusix.knsq.common.NSQException
import com.abusix.knsq.common.NSQTimeoutException
import com.abusix.knsq.common.closeQuietly
import com.abusix.knsq.config.ClientConfig
import com.abusix.knsq.config.ConnectionConfig
import com.abusix.knsq.protocol.Error
import com.abusix.knsq.protocol.Frame
import com.abusix.knsq.protocol.Message
import com.abusix.knsq.protocol.Response
import com.google.common.net.HostAndPort
import kotlinx.serialization.SerializationException
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import org.xerial.snappy.SnappyFramedInputStream
import org.xerial.snappy.SnappyFramedOutputStream
import java.io.*
import java.net.InetSocketAddress
import java.net.Socket
import java.time.Duration
import java.time.Instant
import java.util.concurrent.*
import java.util.zip.Deflater
import java.util.zip.DeflaterOutputStream
import java.util.zip.Inflater
import java.util.zip.InflaterInputStream
import kotlin.math.min


/**
 * An internal class used for generic connection management and its lifecycle. This class should not be used directly.
 * Please use [com.abusix.knsq.publish.Publisher] or [com.abusix.knsq.subscribe.Subscriber] instead.
 */
@Suppress("UnstableApiUsage")
internal abstract class Connection(
    private val clientConfig: ClientConfig, val host: HostAndPort,
    protected val executor: ScheduledExecutorService
) {
    companion object {
        private val logger = LoggerFactory.getLogger(Connection::class.java)
        private val json = Json { encodeDefaults = true; ignoreUnknownKeys = true }
    }

    lateinit var output: DataOutputStream
        private set
    private lateinit var input: DataInputStream

    lateinit var config: ConnectionConfig

    @Volatile
    var isRunning = false
        private set

    var lastActionFlush: Instant = Instant.EPOCH
        private set
    protected var unflushedBytes = 0
    private var lastHeartbeat: Instant = Instant.EPOCH
    protected val responseQueue: BlockingQueue<Frame> = ArrayBlockingQueue(1)
    private var heartbeatTask: ScheduledFuture<*>? = null
    private var lastReceiveHeartbeatTask: Future<*>? = null

    var onNSQError: ((Error) -> Unit)? = null
    var onException: ((Exception) -> Unit)? = null

    private var readThread: Thread? = null

    @Synchronized
    open fun connect() {
        if (isRunning) {
            throw IllegalStateException("Connection is already running")
        }
        isRunning = true

        val sock = Socket()
        sock.soTimeout = clientConfig.readTimeout
        sock.connect(InetSocketAddress(host.host, host.port), clientConfig.connectTimeout)

        val streams = StreamPair(sock.getInputStream(), sock.getOutputStream())
        setStreams(sock.getInputStream(), sock.getOutputStream(), streams)
        output.write("  V2".toByteArray(Charsets.US_ASCII))

        writeCommand("IDENTIFY")
        writeData(json.encodeToString(clientConfig).toByteArray(Charsets.UTF_8))
        output.flush()
        val response = readFrame()
        if (response !is Response) {
            throw KNSQException("Unexpected message type: ${response.type}")
        }

        config = try {
            json.decodeFromString(response.msg)
        } catch (e: SerializationException) {
            throw KNSQException("Received no or bad config from nsqd", e)
        }
        logger.debug("connected connectionConfig: {}", response)

        sock.soTimeout = config.heartbeatInterval.toInt() + 5000

        wrapEncryption(sock, streams)
        wrapCompression(streams)
        if (!streams.isBuffered) {
            setStreams(BufferedInputStream(streams.input), BufferedOutputStream(streams.output), streams)
        }

        sendAuthorization()
        heartbeatTask = executor.scheduleAtFixedRate(
            ::checkHeartbeat, config.heartbeatInterval + 2000L,
            config.heartbeatInterval, TimeUnit.MILLISECONDS
        )
        lastHeartbeat = Instant.now()

        readThread = Thread(::read).apply {
            name = "Reading Thread Connection#${hashCode()}"
            start()
        }
    }

    @Synchronized
    protected fun writeCommand(cmd: String, vararg params: Any) {
        val bytes = if (params.isNotEmpty()) {
            "$cmd ${params.joinToString(" ")}\n"
        } else {
            "$cmd\n"
        }.toByteArray(Charsets.US_ASCII)
        output.write(bytes)
        unflushedBytes += bytes.size
    }

    @Synchronized
    protected fun writeData(data: ByteArray) {
        output.writeInt(data.size)
        output.write(data)
        unflushedBytes += 4
        unflushedBytes += data.size
    }

    protected fun flush() {
        output.flush()
        lastActionFlush = Instant.now()
        unflushedBytes = 0
    }

    @Synchronized
    open fun stop() {
        isRunning = false
        lastReceiveHeartbeatTask?.cancel(true)
        heartbeatTask?.cancel(true)
        readThread?.interrupt()
        output.closeQuietly(logger)
        input.closeQuietly(logger)
        logger.debug("connection closed: $this")
    }

    abstract fun onIncomingMessage(message: Message)

    @Synchronized
    open fun stateDesc(): String {
        val now = Instant.now()
        return "$this lastFlush was ${Duration.between(lastActionFlush, now).toMillis()}ms ago; " +
                "lastHeartbeat was ${Duration.between(lastHeartbeat, now).toMillis()}ms ago;" +
                "unflushedBytes: $unflushedBytes"
    }

    private fun wrapEncryption(baseSocket: Socket, streamPair: StreamPair) {
        if (!config.tls) {
            return
        }
        logger.debug("adding tls")
        val sslSocket = clientConfig.sslSocketFactory.createSocket(
            baseSocket, baseSocket.inetAddress.hostAddress, baseSocket.port, true
        )
        setStreams(sslSocket.getInputStream(), sslSocket.getOutputStream(), streamPair)
        checkIsOK(readFrame())
    }

    private fun wrapCompression(streamPair: StreamPair) {
        if (config.deflate) {
            logger.debug("adding deflate compression")
            val inflateIn = InflaterInputStream(streamPair.input, Inflater(true), 32768)
            val deflateOut = DeflaterOutputStream(
                streamPair.output, Deflater(config.deflateLevel, true), 32768, true
            )
            setStreams(inflateIn, deflateOut, streamPair)
            streamPair.isBuffered = true
            checkIsOK(readFrame())
        } else if (config.snappy) {
            logger.debug("adding snappy compression")
            val snappyIn = SnappyFramedInputStream(streamPair.input)
            val snappyOut = SnappyFramedOutputStream(streamPair.output)
            setStreams(snappyIn, snappyOut, streamPair)
            checkIsOK(readFrame())
        }
    }

    private fun sendAuthorization() {
        if (config.authRequired) {
            if (clientConfig.authSecret.isEmpty()) {
                throw KNSQException("nsqd requires authorization, set ClientConfig.authSecret before connecting")
            }
            writeCommand("AUTH")
            writeData(clientConfig.authSecret)
            output.flush()
            val authResponse = readFrame()
            logger.debug("authorization response: {}", authResponse)
            if (authResponse is Error) {
                throw NSQException(authResponse)
            }
        }
    }

    private fun checkHeartbeat() {
        val isDead: Boolean
        synchronized(this) { isDead = Instant.now().isAfter(lastHeartbeat.plusMillis(2 * config.heartbeatInterval)) }
        if (isDead) {
            logger.info("heartbeat failed, closing connection:{}", toString())
            stop()
        }
    }

    private fun readFrame(): Frame {
        val size = input.readInt()
        return when (val ft = input.readInt()) {
            0 -> Response(input.readAscii(size - 4))
            1 -> Error(input.readAscii(size - 4))
            2 -> Message(
                input.readLong(), input.readUnsignedShort(),
                input.readAscii(16), input.readBytes(size - 30), this
            )
            else -> throw KNSQException("unexpected frame type:$ft")
        }
    }

    private fun read() {
        try {
            while (isRunning) {
                //no need to synchronize, this is the only thread that reads after connect()
                when (val frame = readFrame()) {
                    is Response -> if (frame.msg == "_heartbeat_") {
                        //don't block this thread
                        if (lastReceiveHeartbeatTask?.isDone != false) {
                            lastReceiveHeartbeatTask = executor.submit(::receivedHeartbeat)
                        }
                    } else {
                        responseQueue.offer(frame)
                    }
                    is Error -> {
                        responseQueue.offer(frame)
                        onNSQError?.invoke(frame)
                        if (frame.errorType.terminateConnection) {
                            break
                        }
                    }
                    is Message -> onIncomingMessage(frame)
                }
            }
        } catch (e: Exception) {
            if (e !is InterruptedException && isRunning) {
                onException?.invoke(e)
                logger.warn("Exception while reading. Terminating.", e)
            }
        } finally {
            if (isRunning) {
                stop()
            }
        }
    }

    @Synchronized
    private fun receivedHeartbeat() {
        output.write("NOP\n".toByteArray(Charsets.US_ASCII))
        output.flush() //NOP does not update lastActionFlush
        lastHeartbeat = Instant.now()
    }

    protected fun flushAndReadOK() {
        flush()
        checkIsOK(
            responseQueue.poll(
                min(clientConfig.readTimeout.toLong(), config.heartbeatInterval),
                TimeUnit.MILLISECONDS
            ) ?: throw NSQTimeoutException()
        )
    }

    private fun checkIsOK(frame: Frame) {
        if (frame !is Response || frame.msg != "OK") {
            throw if (frame is Error) {
                NSQException(frame)
            } else {
                KNSQException("bad response: $frame")
            }
        }
    }

    private fun setStreams(input: InputStream, output: OutputStream, streamPair: StreamPair) {
        streamPair.input = input
        streamPair.output = output
        this.input = DataInputStream(input)
        this.output = DataOutputStream(output)
    }

    private fun DataInputStream.readBytes(size: Int): ByteArray {
        val data = ByteArray(size)
        this.readFully(data)
        return data
    }

    private fun DataInputStream.readAscii(size: Int): String {
        return String(readBytes(size), Charsets.US_ASCII)
    }

    private data class StreamPair(
        var input: InputStream,
        var output: OutputStream,
        var isBuffered: Boolean = false
    )
}
