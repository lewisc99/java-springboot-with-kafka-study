package com.consumer.listener;

import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
@Configuration
public class TestListener {
    @KafkaListener(topics = "topic-1",groupId = "group-2")
    public void Listen(String message)
    {
       System.out.println("Received: " + message);
    }
}
