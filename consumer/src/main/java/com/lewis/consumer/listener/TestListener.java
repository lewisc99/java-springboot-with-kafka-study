package com.lewis.consumer.listener;

import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.adapter.ConsumerRecordMetadata;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.TimeZone;


@Configuration
public class TestListener {

    //encapsulate ConsumerRecordMetadata metadata only available kafka -v 2.5
    @KafkaListener(topics = "topic-1",groupId = "group-1")
    public void Listen(String message, ConsumerRecordMetadata metadata)
    {
//       System.out.println("Topic " + metadata.topic() + " partition: " + metadata.partition() + " Message: " + message + " Offset: " + metadata.offset());
//       System.out.println("TimesTamp: " +LocalDateTime.ofInstant(
//               Instant.ofEpochMilli(metadata.timestamp()), TimeZone.getDefault().toZoneId()
//       )); //will compare this timezone with the defined in the controller of Publisher project  before publishing the message

    }
}
