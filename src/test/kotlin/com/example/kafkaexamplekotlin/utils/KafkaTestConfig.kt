package com.example.kafkaexamplekotlin.utils

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.ProducerFactory

const val BOOTSTRAP_SERVER = "localhost:9099"
const val BOOTSTRAP_SERVER_PORT = "9099"

@Configuration
class KafkaTestConfig {

    val producerProperties = mapOf<String, Any>(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to BOOTSTRAP_SERVER,
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to KafkaAvroSerializer::class.java,
        "schema.registry.url" to "http://mock:8081"
    )

    val consumerProperties = mapOf<String, Any>(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to BOOTSTRAP_SERVER,
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
        ConsumerConfig.GROUP_ID_CONFIG to "test-consumer",
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to KafkaAvroDeserializer::class.java,
        KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG to true,
        "schema.registry.url" to "http://mock:8081"
    )

    @Bean
    fun producerFactory(): ProducerFactory<String, Any> =
        DefaultKafkaProducerFactory(producerProperties, StringSerializer(), kafkaAvroSerializer())

    @Bean
    fun kafkaAvroSerializer(): Serializer<Any> {
        return KafkaAvroSerializer(schemaRegistryClient(), producerProperties)
    }

    @Bean
    fun kafkaAvroDeserializer(): KafkaAvroDeserializer {
        return KafkaAvroDeserializer(schemaRegistryClient(), consumerProperties)
    }

    @Bean
    fun consumerFactory(): ConsumerFactory<String, Any> {
        val kafkaAvroDeserializer = KafkaAvroDeserializer(schemaRegistryClient(), consumerProperties)
        return DefaultKafkaConsumerFactory(consumerProperties, StringDeserializer(), kafkaAvroDeserializer)
    }

    @Bean
    fun schemaRegistryClient() = MockSchemaRegistryClient()
}