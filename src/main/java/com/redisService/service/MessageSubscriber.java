package com.redisService.service;

import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.stereotype.Service;

@Service
public class MessageSubscriber implements MessageListener {
    @Override
    public void onMessage(Message message, byte[] pattern) {
        String msg = new String(message.getBody());
        System.out.println("Receive Message ===> "+msg);
    }
}
