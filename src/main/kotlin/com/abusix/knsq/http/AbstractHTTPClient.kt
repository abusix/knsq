package com.abusix.knsq.http

import com.abusix.knsq.common.NSQHTTPResponseException
import com.abusix.knsq.subscribe.Subscriber
import com.google.common.net.HostAndPort
import kotlinx.serialization.json.Json
import java.net.HttpURLConnection
import java.net.URL
import java.time.Duration

@Suppress("UnstableApiUsage")
abstract class AbstractHTTPClient(
    private val host: HostAndPort,
    private val tls: Boolean = false,
    private val connectTimeout: Duration = Subscriber.DEFAULT_LOOKUP_INTERVAL.dividedBy(2),
    private val readTimeout: Duration = Subscriber.DEFAULT_LOOKUP_INTERVAL.dividedBy(2)
) {
    companion object {
        val json = Json { ignoreUnknownKeys = true }
    }

    protected fun performGET(path: String): HttpURLConnection {
        val urlString = "http${if (tls) "s" else ""}://$host/$path"
        val con = URL(urlString).openConnection() as HttpURLConnection
        con.connectTimeout = connectTimeout.toMillis().toInt()
        con.readTimeout = readTimeout.toMillis().toInt()

        if (con.responseCode != 200) {
            throw NSQHTTPResponseException(con.responseCode, "host: $host")
        }
        return con
    }

    protected fun performPOST(path: String, body: ByteArray = byteArrayOf()): HttpURLConnection {
        val urlString = "http://$host/$path"
        val con = URL(urlString).openConnection() as HttpURLConnection
        con.requestMethod = "POST"
        con.connectTimeout = connectTimeout.toMillis().toInt()
        con.readTimeout = readTimeout.toMillis().toInt()
        if (body.isNotEmpty()) {
            con.doOutput = true
            con.outputStream.use { it.write(body) }
        }

        if (con.responseCode != 200) {
            throw NSQHTTPResponseException(con.responseCode, "host: $host")
        }
        return con
    }

    /**
     * Check whether or not the endpoint is available.
     */
    fun ping() = try {
        performGET("ping")
        true
    } catch (e: Exception) {
        false
    }
}