package com.example.kafkaexamplekotlin.config.kafka

import com.example.events.ErrorEvent
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.listener.CommonErrorHandler
import org.springframework.kafka.listener.MessageListenerContainer
import org.springframework.stereotype.Component

@Component
class KafkaErrorHandler(
    private val kafkaTemplate: KafkaTemplate<String, Any>,
    private val kafkaConfigurationProperties: KafkaConfigurationProperties
) : CommonErrorHandler {

    companion object {
        private val logger = LoggerFactory.getLogger(this::class.java)
    }

    override fun handleRecord(
        thrownException: Exception,
        record: ConsumerRecord<*, *>,
        consumer: Consumer<*, *>,
        container: MessageListenerContainer
    ) {
        logger.error("Error ${thrownException.message} occurred while handling record: ${record.value()}")
        val errorEvent = ErrorEvent("Error message", "Error code")
        kafkaTemplate.send(kafkaConfigurationProperties.dltTopicName, record.key().toString(), errorEvent)
    }
}
