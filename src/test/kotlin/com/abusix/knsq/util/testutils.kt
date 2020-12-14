package com.abusix.knsq.util

import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName

class KGenericContainer(image: DockerImageName) : GenericContainer<KGenericContainer>(image)