import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

tasks.withType(KotlinCompile::class) {
    kotlinOptions.jvmTarget = JavaVersion.VERSION_11.toString()
}

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}

plugins {
    kotlin("jvm") version "1.9.20"
    kotlin("plugin.serialization") version "1.9.20"
    id("org.jetbrains.dokka") version "1.9.20"
    java
    id("org.gradle.test-retry") version "1.4.0"
    id("com.github.ben-manes.versions") version "0.42.0"
    `maven-publish`
    signing
}

group = "com.abusix"
version = file("VERSION").readText().trim()

repositories {
    mavenCentral()
}

dependencies {
    implementation(kotlin("stdlib"))
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.6.3")

    implementation("com.google.guava:guava:33.2.1-jre")
    implementation("org.slf4j:slf4j-api:2.0.13")
    implementation("org.xerial.snappy:snappy-java:1.1.10.4")

    testImplementation(kotlin("test"))
    testImplementation(kotlin("test-junit5"))
    testImplementation(kotlin("reflect"))
    testImplementation("io.mockk:mockk:1.13.11")
    testImplementation("org.junit.jupiter:junit-jupiter-params:5.8.2")
    testImplementation("org.junit.jupiter:junit-jupiter-engine:5.8.2")
    testImplementation("org.testcontainers:testcontainers:1.19.8")
    testImplementation("com.github.tomakehurst:wiremock:3.0.1")
    testImplementation("ch.qos.logback:logback-core:1.4.14")
    testImplementation("ch.qos.logback:logback-classic:1.4.12")
    dokkaJavadocPlugin("org.jetbrains.dokka:kotlin-as-java-plugin:1.9.20")
}

tasks.processResources {
    filesMatching("knsq.properties") {
        expand(project.properties)
    }
}

tasks.withType<Test> {
    jvmArgs(
        "--add-opens", "java.base/java.io=ALL-UNNAMED"
    )
    useJUnitPlatform { }
    retry {
        failOnPassedAfterRetry.set(false)
        maxFailures.set(3)
        maxRetries.set(3)
    }
}

tasks.register<Jar>("dokkaJavadocJar") {
    group = "Build"
    description = "Assembles a jar archive containing Javadoc documentation."
    dependsOn("dokkaJavadoc")
    archiveClassifier.set("javadoc")
    from("${layout.buildDirectory}/dokka/javadoc")
}

tasks.register<Jar>("sourcesJar") {
    group = "Build"
    description = "Assembles a jar archive containing the main source code."
    archiveClassifier.set("sources")
    from("src/main/kotlin")
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            groupId = "com.abusix"
            artifactId = "knsq"
            from(components["java"])

            artifact(tasks["sourcesJar"])
            artifact(tasks["dokkaJavadocJar"])

            pom {
                name.set("com.abusix:knsq")
                description.set("A NSQ client library written in Kotlin, based on nsq-j")
                url.set("https://github.com/abusix/knsq")

                licenses {
                    license {
                        name.set("MIT License")
                        url.set("http://www.opensource.org/licenses/mit-license.php")
                    }
                }
                developers {
                    developer {
                        id.set("bgeisberger")
                        name.set("Bernhard Geisberger")
                        email.set("bernhard.geisberger@abusix.com")
                        organization.set("Abusix")
                        organizationUrl.set("https://www.abusix.com/")
                    }
                }
                scm {
                    connection.set("scm:git:git://github.com/abusix/knsq.git")
                    developerConnection.set("scm:git:ssh://github.com:abusix/knsq.git")
                    url.set("https://github.com/abusix/knsq/tree/master")
                }
            }
        }
    }
    repositories {
        maven {
            url = uri("https://oss.sonatype.org/service/local/staging/deploy/maven2/")
            if (project.hasProperty("ossrhUsername") && project.hasProperty("ossrhPassword")) {
                credentials {
                    username = project.properties["ossrhUsername"].toString()
                    password = project.properties["ossrhPassword"].toString()
                }
            }
        }
    }
}

signing {
    if (project.hasProperty("signing.keyId")
        && project.hasProperty("signing.password")
        && project.hasProperty("signing.secretKeyRingFile")
    ) {
        sign(publishing.publications["maven"])
    }
}