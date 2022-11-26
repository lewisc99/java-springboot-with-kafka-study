package com.lewis.consumer.listener;

import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;


@Configuration
public class TestListener {

    @KafkaListener(topics = "topic-1",groupId = "group-1")
    public void Listen(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                       @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition)
    {
       System.out.println("Topic " + topic + " partition: " + partition + " Message: " + message);
    }
}
