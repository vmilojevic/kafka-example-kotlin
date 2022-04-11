package com.example.kafkaexamplekotlin.config.kafka

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate


@Configuration
@EnableKafka
@EnableConfigurationProperties(KafkaConfigurationProperties::class)
class KafkaConfiguration(
    private val kafkaConfigurationProperties: KafkaConfigurationProperties
) {

    val producerConfig: Map<String, Any> = mapOf(
        ProducerConfig.ACKS_CONFIG to "all",
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaConfigurationProperties.bootstrapServers,
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to KafkaAvroSerializer::class.java,
        ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG to kafkaConfigurationProperties.deliveryTimeoutMs,
        "schema.registry.url" to kafkaConfigurationProperties.schemaRegistryUrl
    )

    val consumerConfig: Map<String, Any> = mapOf(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaConfigurationProperties.bootstrapServers,
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to KafkaAvroDeserializer::class.java,
        KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG to true,
        "schema.registry.url" to kafkaConfigurationProperties.schemaRegistryUrl
    )

    val sslConfig: Map<String, Any> = if (kafkaConfigurationProperties.sslEnabled) {
        val kafkaSslPasswords =
            this.getKafkaSslPasswordFromSecretManager(kafkaConfigurationProperties.sslPasswordsSecretId)
        val sslProps = mapOf<String, Any>(
            CommonClientConfigs.SECURITY_PROTOCOL_CONFIG to "SSL",
            SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG to kafkaConfigurationProperties.sslKeystorePath,
            SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG to kafkaSslPasswords.keystorePassword,
            SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG to kafkaConfigurationProperties.sslTruststorePath,
            SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG to kafkaSslPasswords.truststorePassword,
        )
        val schemaRegSSLProps = sslProps.filterKeys { it != CommonClientConfigs.SECURITY_PROTOCOL_CONFIG }
            .mapKeys { "schema.registry.${it.key}" }

        // returning
        sslProps + schemaRegSSLProps
    } else {
        mapOf()
    }

    // Producer configuration
    @Bean
    fun kafkaTemplate(): KafkaTemplate<String, Any> =
        KafkaTemplate(
            DefaultKafkaProducerFactory(producerConfig + sslConfig)
        )

    @Bean
    fun exampleTopic(): NewTopic {
        return TopicBuilder.name(kafkaConfigurationProperties.topicName)
            .partitions(kafkaConfigurationProperties.numberOfPartitions)
            .replicas(kafkaConfigurationProperties.replicationFactor)
            .compact()
            .build()
    }

    @Bean
    fun dltExampleTopic(): NewTopic {
        return TopicBuilder.name(kafkaConfigurationProperties.dltTopicName)
            .partitions(kafkaConfigurationProperties.numberOfPartitions)
            .replicas(kafkaConfigurationProperties.replicationFactor)
            .build()
    }

    // Consumer configuration
    @Bean
    fun consumerFactory(): ConsumerFactory<String, Any> =
        DefaultKafkaConsumerFactory(
            consumerConfig + sslConfig
        )

    @Bean
    fun kafkaListenerContainerFactory(errorHandler: KafkaErrorHandler): ConcurrentKafkaListenerContainerFactory<String, Any> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, Any>()
        factory.consumerFactory = consumerFactory()
        factory.setCommonErrorHandler(errorHandler)
        return factory
    }

    // Private methods
    private fun getKafkaSslPasswordFromSecretManager(secretId: String): KafkaSslPasswords {
        // Here we can use the AWS Secrets Manager to retrieve passwords
        return KafkaSslPasswords("test", "test")
    }

    data class KafkaSslPasswords(
        val truststorePassword: String,
        val keystorePassword: String,
    )
}
