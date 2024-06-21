# knsq

[![Maven Central](https://img.shields.io/maven-central/v/com.abusix/knsq)](https://repo1.maven.org/maven2/com/abusix/knsq/)
[![GitHub issues](https://img.shields.io/github/issues/abusix/knsq)](https://github.com/abusix/knsq/issues)
[![JDK Version](https://img.shields.io/badge/jdk-11-blue)](#)
[![Kotlin Version](https://img.shields.io/badge/kotlin-1.9.20-blue)](#)
[![GitHub](https://img.shields.io/github/license/abusix/knsq)](#)

A NSQ client library written in Kotlin, some parts based on [nsqj](https://github.com/sproutsocial/nsq-j).

## Features

* Subscribers with nsqlookupd support
* Publishers
* HTTP API wrapper for nsqd and nsqlookupd
* Subscriber backoff
* TLS support
* Snappy and DEFLATE compression
* Sampling
* Authorization
* Full error and exception control
* Scalable

## Maven and gradle configuration

Add the dependency using Maven

```xml
<dependency>
    <groupId>com.abusix</groupId>
    <artifactId>knsq</artifactId>
    <version>1.5.0</version>
</dependency>
```

or Gradle

```
dependencies {
  compile 'com.abusix:knsq:1.5.0'
}
```

## Getting started

### Publish data

```kotlin
val pub = Publisher("nsqd.example.org:4150")

pub.onException = {
    // handle async exceptions here
    println(it)
}

// sync publish
pub.publish("sample.topic", "Hello!".toByteArray())

// async buffered publish for better performance and throughput
pub.publishBuffered("sample.other", "Hi, I am async!".toByteArray())

pub.stop()
```

Publishers are low-level, non self-recovering instances. Once a connection is broken or closed, they will not reconnect
automatically. Calls to `Publisher.connect()` will not succeed afterwards. If a connection has to be re-established, a
new instance of `Publisher` has to be created.

Publishers can be configured the following ways:

* Constructor parameters (see below)
* Individual Batcher configurations, accessible via `Publisher.getBatcher(topic)`

### Subscribe

```kotlin
val subscriber = Subscriber("nsqlookupd.example.org:4161")

val subscription = subscriber.subscribe("sample.topic", "channel", { msg ->
    println(msg)
}, onFailedMessage = { msg ->
    println("Message failed too many times: $msg")
}, onException = { e ->
    // exception happened on client-side
    println(e)
}, onNSQError = { e ->
    // nsq returned unexpected error
    println(e)
})
```

Subscribers are high-level, self-recovering instances. Broken connections will be re-established and new nsqd instances
will be discovered via nsqlookupd dynamically.

You can control the message flow manually using several configuration options on the `Subscription` object returned
by `subscriber.subscribe`. Further configuration is available on the `Subscriber` object and its constructor. Messages
can also be controlled directly using methods on the message object:

```kotlin
val subscription = subscriber.subscribe("sample.topic", "channel", onMessage = { msg ->
    msg.requeue() // requeue the message, with optional delay
    msg.finish() // tell nsqd that you received the message
    msg.touch() // reset server-side timeouts
})
```

### Directly subscribe to nsqd

As an alternative to nsqlookupd, it is also possible to directly connect to a nsqd instance using `DirectSubscriber`.
Those objects are self-recovering from errors as well and support all operations of normal `Subscriber` objects.

```kotlin
val subscriber = DirectSubscriber("nsqd.example.org:4150")

val subscription = subscriber.subscribe("sample.topic", "channel", onMessage = { msg ->
    println(msg)
})
```

### Customizing connection parameters

`Publisher`, `Subscriber` and `DirectSubscriber` can be customized by providing `ClientConfig` objects via the
constructor. Most of the parameters in this config are immutable and must be created with the object before creating
a `Publisher`,
`Subscriber` or `DirectSubscriber`.

```kotlin
val clientConfig = ClientConfig(
    clientId = "sampleId",
    authSecret = "IAMASECRET".toByteArray(),
    tls = false
)

val pub = Publisher("nsqd.example.org:4150", clientConfig = clientConfig)
```

For the full set of options in `ClientConfig` and their default values, please see the Dokka or JavaDoc documentation
below.

## Further Documentation

* Dokka: https://abusix.github.io/knsq/html
* JavaDoc: https://abusix.github.io/knsq/javadoc

## License

This library is licensed under the MIT License.