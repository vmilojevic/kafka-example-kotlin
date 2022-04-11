package com.example.kafkaexamplekotlin.consumer

import com.example.events.ErrorEvent
import com.example.events.ExampleEvent
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class KafkaConsumer {

    companion object {
        private val logger = LoggerFactory.getLogger(this::class.java)
    }

    @KafkaListener(topics = ["\${example.kafka.topic-name}"], groupId = "\${example.kafka.group-id}")
    fun processMessage(message: ExampleEvent) {
        logger.info("Received a message!")
        logger.info(message.toString())

        if (message.getExampleFieldOne() == "error") {
            throw Exception("Error occurred while processing the record.")
        }
    }

    @KafkaListener(topics = ["\${example.kafka.dlt-topic-name}"], groupId = "\${example.kafka.group-id}")
    fun processDltMessage(message: ErrorEvent) {
        logger.info("Received an error message!")
        logger.info(message.toString())
    }
}
