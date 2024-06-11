package com.abusix.knsq.publish

import com.abusix.knsq.config.ClientConfig
import com.abusix.knsq.util.IntegrationTestBase
import com.abusix.knsq.util.KGenericContainer
import org.junit.jupiter.api.Timeout
import org.testcontainers.containers.Network
import org.testcontainers.containers.output.OutputFrame
import org.testcontainers.utility.DockerImageName
import org.testcontainers.utility.MountableFile
import java.security.KeyStore
import java.time.Duration
import java.util.concurrent.TimeUnit
import javax.net.ssl.SSLContext
import javax.net.ssl.TrustManagerFactory
import kotlin.test.*

@Timeout(30, unit = TimeUnit.SECONDS)
class PublisherIntegrationTest : IntegrationTestBase() {
    private val network = Network.newNetwork()!!
    private val nsqD: KGenericContainer = KGenericContainer(DockerImageName.parse("nsqio/nsq"))
        .withCopyToContainer(MountableFile.forClasspathResource("cert.pem"), "/etc/ssl/certs/cert.pem")
        .withCopyToContainer(MountableFile.forClasspathResource("key.pem"), "/etc/ssl/certs/key.pem")
        .withCommand("/nsqd --tls-cert=/etc/ssl/certs/cert.pem --tls-key=/etc/ssl/certs/key.pem")
        .withNetwork(network)
        .withNetworkAliases("nsqd")
        .withExposedPorts(4150)
    private val subscriber: KGenericContainer = KGenericContainer(DockerImageName.parse("nsqio/nsq"))
        .withCommand(
            "/nsq_tail -topic=pubtest -channel=tail#ephemeral " +
                    "-nsqd-tcp-address=nsqd:4150"
        )
        .withNetwork(network)
    private val received = mutableSetOf<String>()
    private var buffer = byteArrayOf()

    @BeforeTest
    fun prepareContainers() {
        nsqD.start()
        subscriber.start()
        subscriber.followOutput {
            if (it.type == OutputFrame.OutputType.STDOUT) {
                buffer += it.bytes
            }
            val lines = buffer.decodeToString().split('\n')
            buffer = lines.last().toByteArray()
            received.addAll(lines.take(lines.size - 1))
        }
    }

    @AfterTest
    fun cleanup() {
        subscriber.stop()
        nsqD.stop()
    }

    @Test
    fun testPublish() {
        val sent = mutableSetOf<String>()
        val pub = Publisher(nsqD.host + ":" + nsqD.getMappedPort(4150))

        generateMessages(200, delayChance = 0.1, maxDelay = 1000, onMessage = {
            if (it !in sent) {
                sent.add(it)
                pub.publish("pubtest", it.toByteArray())
            }
        })

        while (received.size < sent.size) {
            Thread.sleep(100)
        }

        pub.stop()
        assertEquals(received, sent)
    }

    @Test
    fun testPublishBuffered() {
        val sent = mutableSetOf<String>()
        val pub = Publisher(nsqD.host + ":" + nsqD.getMappedPort(4150))
        pub.getBatcher("pubtest").maxSize = 501

        generateMessages(1000, delayChance = 0.015, maxDelay = 1200, onMessage = {
            if (it !in sent) {
                sent.add(it)
                pub.publishBuffered("pubtest", it.toByteArray())
            }
        })

        while (received.size < sent.size) {
            Thread.sleep(100)
        }

        pub.stop()
        assertEquals(received, sent)
    }

    @Test
    fun testPublishDeferred() {
        val sent = mutableSetOf<String>()
        val pub = Publisher(nsqD.host + ":" + nsqD.getMappedPort(4150))

        generateMessages(200, {
            if (it !in sent) {
                sent.add(it)
                pub.publishDeferred("pubtest", it.toByteArray(), Duration.ofSeconds(1))
            }
        })

        while (received.size < sent.size) {
            Thread.sleep(100)
        }

        pub.stop()
        assertEquals(received, sent)
    }

    @Test
    fun testPublishMultiple() {
        val sent = mutableSetOf<String>()
        val pub = Publisher(nsqD.host + ":" + nsqD.getMappedPort(4150))

        generateMessages(100, { sent.add(it) })
        pub.publishMultiple("pubtest", sent.map { it.toByteArray() })

        while (received.size < sent.size) {
            Thread.sleep(100)
        }

        pub.stop()
        assertEquals(received, sent)
    }

    @Test
    fun testSnappy() {
        val sent = mutableSetOf<String>()
        val clientConfig = ClientConfig(snappy = true)
        val pub = Publisher(nsqD.host + ":" + nsqD.getMappedPort(4150), clientConfig)
        pub.getBatcher("pubtest").maxSize = 501

        generateMessages(1000, {
            if (it !in sent) {
                sent.add(it)
                pub.publishBuffered("pubtest", it.toByteArray())
            }
        })

        while (received.size < sent.size) {
            Thread.sleep(100)
        }

        pub.stop()
        assertEquals(received, sent)
    }

    @Test
    fun testDeflate() {
        val sent = mutableSetOf<String>()
        val clientConfig = ClientConfig(deflate = true)
        val pub = Publisher(nsqD.host + ":" + nsqD.getMappedPort(4150), clientConfig)
        pub.getBatcher("pubtest").maxSize = 501

        generateMessages(1000, {
            if (it !in sent) {
                sent.add(it)
                pub.publishBuffered("pubtest", it.toByteArray())
            }
        })

        while (received.size < sent.size) {
            Thread.sleep(100)
        }

        pub.stop()
        assertEquals(received, sent)
    }

    /*
    To generate new certificates:
    openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem -days 30065 -nodes
    keytool -import -file cert.pem -alias server -keystore server.jks
     */
    @Test
    fun testEncryption() {
        val keyStore = KeyStore.getInstance(KeyStore.getDefaultType())
        keyStore.load(javaClass.getResourceAsStream("/server.jks"), "password".toCharArray())

        val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
        tmf.init(keyStore)

        val ctx = SSLContext.getInstance("TLS")
        ctx.init(null, tmf.trustManagers, null)

        val sent = mutableSetOf<String>()
        val clientConfig = ClientConfig(tls = true, sslSocketFactory = ctx.socketFactory)
        val pub = Publisher(nsqD.host + ":" + nsqD.getMappedPort(4150), clientConfig)
        pub.getBatcher("pubtest").maxSize = 501

        generateMessages(1000, {
            if (it !in sent) {
                sent.add(it)
                pub.publishBuffered("pubtest", it.toByteArray())
            }
        })

        while (received.size < sent.size) {
            Thread.sleep(100)
        }

        pub.stop()
        assertEquals(received, sent)
    }

    @Test
    fun testDeadConnection() {
        val pub = Publisher(nsqD.host + ":" + nsqD.getMappedPort(4150))
        generateMessages(10, { pub.publish("pubtest", it.toByteArray()) })
        nsqD.stop()
        assertFailsWith<Exception> { pub.publish("pubtest", "shouldFail".toByteArray()) }
    }
}