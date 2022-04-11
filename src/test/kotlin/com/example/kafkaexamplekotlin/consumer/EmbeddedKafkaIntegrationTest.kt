package com.example.kafkaexamplekotlin.consumer

import com.example.events.ExampleEvent
import com.example.kafkaexamplekotlin.config.kafka.KafkaConfigurationProperties
import com.example.kafkaexamplekotlin.producer.KafkaProducer
import com.example.kafkaexamplekotlin.utils.BOOTSTRAP_SERVER
import com.example.kafkaexamplekotlin.utils.BOOTSTRAP_SERVER_PORT
import com.example.kafkaexamplekotlin.utils.KafkaTestConfig
import org.apache.kafka.clients.consumer.Consumer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.kafka.test.utils.KafkaTestUtils
import org.springframework.test.annotation.DirtiesContext
import org.assertj.core.api.Assertions.assertThat

@SpringBootTest(
    classes = [KafkaTestConfig::class],
    properties = ["spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration"]
)
@DirtiesContext
@EmbeddedKafka(
    partitions = 1,
    brokerProperties = ["listeners=PLAINTEXT://$BOOTSTRAP_SERVER", "port=$BOOTSTRAP_SERVER_PORT"]
)
class EmbeddedKafkaIntegrationTest(
    @Autowired producerFactory: ProducerFactory<String, Any>,
    @Autowired val consumerFactory: ConsumerFactory<String, Any>
) {
    private lateinit var testConsumer: Consumer<String, Any>
    private lateinit var dltTestConsumer: Consumer<String, Any>
    val kafkaTemplate = KafkaTemplate(producerFactory)

    val testKafkaConfig = KafkaConfigurationProperties(
        "test-topic",
        "test-topic.dlt",
        "localhost:9099",
        35000,
        "http://mock:8081",
        1,
        1,
        false
    )

    @BeforeEach
    fun setUp() {
        testConsumer = consumerFactory.createConsumer()
        testConsumer.subscribe(
            listOf(
                testKafkaConfig.topicName
            )
        )
        dltTestConsumer = consumerFactory.createConsumer()
        dltTestConsumer.subscribe(
            listOf(
                testKafkaConfig.dltTopicName
            )
        )
    }

    @AfterEach
    fun cleanUp() {
        testConsumer.close()
        dltTestConsumer.close()
    }

    @Test
    fun `can write and read events from kafka`() {
        val producer = KafkaProducer(kafkaTemplate, testKafkaConfig)
        val event = ExampleEvent("error", "field2")
        producer.publishEvent(event)

        val latestEvent = KafkaTestUtils
            .getRecords(testConsumer)
            .records(testKafkaConfig.topicName)
            .last()

        assertThat(latestEvent.key()).isEqualTo(event.getExampleFieldOne())
        assertThat(latestEvent.value()).isInstanceOf(ExampleEvent::class.java)
        val dto = latestEvent.value() as ExampleEvent
        assertThat(dto.getExampleFieldOne()).isEqualTo(event.getExampleFieldOne())
        assertThat(dto.getExampleFieldTwo()).isEqualTo(event.getExampleFieldTwo())
    }
}
