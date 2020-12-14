package com.abusix.knsq.http.model

import kotlinx.serialization.Serializable

@Serializable
data class TopicsResponse(
    val topics: List<String> = listOf()
)