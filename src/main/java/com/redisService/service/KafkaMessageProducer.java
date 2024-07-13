package com.redisService.service;

import lombok.AllArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Service;

@Service
@AllArgsConstructor
public class KafkaMessageProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

//    private final ReactiveKafkaProducerTemplate<String, String> reactiveKafkaProducerTemplate;

    public void sendMessage(String msg){
        kafkaTemplate.send("my-topic", msg);
    }
}
