package com.example.kafkaexamplekotlin.controller

import com.example.events.ExampleEvent
import com.example.kafkaexamplekotlin.producer.KafkaProducer
import org.slf4j.LoggerFactory
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RestController

@RestController
class ExampleController(
    private val producer: KafkaProducer
) {

    companion object {
        private val logger = LoggerFactory.getLogger(this::class.java)
    }

    @PostMapping("/publish")
    fun valid(): ResponseEntity<Any> {
        logger.info("Valid endpoint!")
        val event = ExampleEvent("test1", "test2")
        producer.publishEvent(event)
        return ResponseEntity.ok("OK")
    }

    @PostMapping("/publish-error")
    fun error(): ResponseEntity<Any> {
        logger.info("Error endpoint!")
        val event = ExampleEvent("error", "error")
        producer.publishEvent(event)
        return ResponseEntity.ok("OK")
    }
}
