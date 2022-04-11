package com.example.kafkaexamplekotlin.config.kafka

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding

@ConstructorBinding
@ConfigurationProperties(prefix = "example.kafka")
data class KafkaConfigurationProperties(
    val topicName: String,
    val dltTopicName: String,
    val bootstrapServers: String,
    val deliveryTimeoutMs: Int,
    val schemaRegistryUrl: String,
    val numberOfPartitions: Int,
    val replicationFactor: Int,
    val sslEnabled: Boolean,
    val sslKeystorePath: String = "",
    val sslTruststorePath: String = "",
    val sslPasswordsSecretId: String = "",
)
