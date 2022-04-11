package com.example.kafkaexamplekotlin.producer

import com.example.events.ExampleEvent
import com.example.kafkaexamplekotlin.config.kafka.KafkaConfigurationProperties
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service

@Service
class KafkaProducer(
    private val template: KafkaTemplate<String, Any>,
    private val properties: KafkaConfigurationProperties
) {

    companion object {
        private val logger = LoggerFactory.getLogger(this::class.java)
    }

    fun publishEvent(event: ExampleEvent) {
        logger.info("Publishing event $event")
        template.send(properties.topicName, event.getExampleFieldOne(), event)
    }
}
