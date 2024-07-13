package com.redisService.service;

import lombok.AllArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@AllArgsConstructor
public class KafkaMessageConsumer {

    @KafkaListener(topics = "my-topic", groupId = "my-group")
    public void handleMessage(String message){
        System.out.println("Received message=> "+message);
    }

}
