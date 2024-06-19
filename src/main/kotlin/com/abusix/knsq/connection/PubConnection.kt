package com.abusix.knsq.connection

import com.abusix.knsq.config.ClientConfig
import com.abusix.knsq.protocol.Message
import com.google.common.net.HostAndPort
import java.time.Duration
import java.util.concurrent.ScheduledExecutorService


/**
 * An internal class used for connection management and its lifecycle when using a publishing connection.
 * This class should not be used directly. Please use [com.abusix.knsq.publish.Publisher] instead.
 */
internal class PubConnection(clientConfig: ClientConfig, host: HostAndPort, executor: ScheduledExecutorService) :
    Connection(clientConfig, host, executor) {
    @Synchronized
    fun publish(topic: String, data: ByteArray) {
        responseQueue.clear()
        writeCommand("PUB", topic)
        writeData(data)
        flushAndReadOK()
    }

    @Synchronized
    fun publishDeferred(topic: String, data: ByteArray, delay: Duration) {
        responseQueue.clear()
        writeCommand("DPUB", topic, delay.toMillis())
        writeData(data)
        flushAndReadOK()
    }

    @Synchronized
    fun publishMultiple(topic: String, dataList: List<ByteArray>) {
        responseQueue.clear()
        writeCommand("MPUB", topic)
        var bodySize = 4
        for (data in dataList) {
            bodySize += data.size + 4
        }
        output.writeInt(bodySize)
        output.writeInt(dataList.size)
        for (data in dataList) {
            writeData(data)
        }
        flushAndReadOK()
    }

    override fun onIncomingMessage(message: Message) {
        throw IllegalStateException("PUB Connection is not allowed to receive messages!")
    }

    override fun toString() = "PubCon: ${host.host}"

}