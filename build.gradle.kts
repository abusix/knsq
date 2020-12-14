import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

tasks.withType(KotlinCompile::class) {
    kotlinOptions.jvmTarget = JavaVersion.VERSION_1_8.toString()
}

plugins {
    kotlin("jvm") version "1.4.21"
    kotlin("plugin.serialization") version "1.4.21"
    id("org.jetbrains.dokka") version "1.4.20"
    jacoco
    java
    id("org.gradle.test-retry") version "1.2.0"
    id("com.github.ben-manes.versions") version "0.36.0"
    `maven-publish`
}

group = "com.abusix"
version = file("VERSION").readText().trim()

sourceSets.main {
    resources.srcDir("src/main/resources")
}
sourceSets.test {
    resources.srcDir("src/test/resources")
}

repositories {
    jcenter()
}

dependencies {
    implementation(kotlin("stdlib-jdk8"))
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.0.1")

    implementation("com.google.guava:guava:30.0-jre")
    implementation("org.slf4j:slf4j-api:2.0.0-alpha1")
    implementation("org.xerial.snappy:snappy-java:1.1.8.2")

    testImplementation(kotlin("test"))
    testImplementation(kotlin("test-junit5"))
    testImplementation(kotlin("reflect"))
    testImplementation("io.mockk:mockk:1.10.3-jdk8")
    testImplementation("org.junit.jupiter:junit-jupiter-params:5.7.0")
    testImplementation("org.junit.jupiter:junit-jupiter-engine:5.7.0")
    testImplementation("org.testcontainers:testcontainers:1.15.0")
    testImplementation("com.github.tomakehurst:wiremock:2.27.2")
    testImplementation("ch.qos.logback:logback-core:1.3.0-alpha5")
    testImplementation("ch.qos.logback:logback-classic:1.3.0-alpha5")
    dokkaJavadocPlugin("org.jetbrains.dokka:kotlin-as-java-plugin:1.4.20")
}

tasks.processResources {
    filesMatching("knsq.properties") {
        expand(project.properties)
    }
}

tasks.test {
    finalizedBy(tasks.jacocoTestReport)
}
tasks.jacocoTestReport {
    dependsOn(tasks.test)
}

tasks.withType<JacocoReport> {
    classDirectories.setFrom(
        sourceSets.main.get().output.asFileTree.matching {
            exclude(
                "com/abusix/knsq/config/**/*",
                "com/abusix/knsq/http/model/**/*"
            )
        }
    )
}

tasks.withType<Test> {
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
    from("$buildDir/dokka/javadoc")
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
        }
    }
    repositories {
        maven {
            url = uri("https://gitlab.com/api/v4/projects/23097544/packages/maven")
            credentials(HttpHeaderCredentials::class) {
                name = "Job-Token"
                value = System.getenv("CI_JOB_TOKEN")
            }
            authentication {
                create<HttpHeaderAuthentication>("header")
            }
        }
    }
}