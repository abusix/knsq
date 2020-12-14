package com.abusix.knsq.subscribe

import com.abusix.knsq.util.IntegrationTestBase
import java.net.HttpURLConnection
import java.net.URL
import java.net.URLEncoder

open class SubscriberIntegrationTestBase : IntegrationTestBase() {

    private fun post(host: String, topic: String, command: String, body: String) {
        val url = URL("http://$host/$command?topic=" + URLEncoder.encode(topic, "UTF-8"))
        val con = url.openConnection() as HttpURLConnection
        con.requestMethod = "POST"
        con.doOutput = true
        con.outputStream.use { it.write(body.toByteArray()) }
        con.responseCode // to actually send the request
    }

    fun postMessages(host: String, topic: String, vararg msgs: String) {
        var i = 0
        while (i < msgs.size) {
            if (random.nextFloat() < 0.05) {
                post(host, topic, "pub", msgs[i])
                i++
            } else {
                val count = 1 + random.nextInt(40)
                val body = msgs.slice(i until msgs.size.coerceAtMost(i + count)).joinToString("\n")
                post(host, topic, "mpub", body)
                i += count
            }
            if (random.nextFloat() < 0.5) {
                Thread.sleep(random.nextLong(1000))
            }
        }
    }
}