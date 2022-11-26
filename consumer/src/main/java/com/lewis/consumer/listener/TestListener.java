package com.lewis.consumer.listener;

import com.lewis.consumer.custom.PersonCustomListener;
import com.lewis.consumer.models.Person;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.adapter.ConsumerRecordMetadata;

@Configuration
public class TestListener {

    //encapsulate ConsumerRecordMetadata metadata only available kafka -v 2.5
    @KafkaListener(topics = "topic-1",groupId = "group-1")
    public void listen(String message, ConsumerRecordMetadata metadata)
    {
     System.out.println("Topic " + metadata.topic() + " partition: " + metadata.partition() + " Message: " + message + " Offset: " + metadata.offset());
        System.out.println("--------------------------------------------------------------------");

    }

   // @KafkaListener(topics = "person-topic",groupId = "group-1", containerFactory = "personKafkaListenerContainerFactory")
    @PersonCustomListener(groupId = "group-1")
    public void createPerson(Person person, ConsumerRecordMetadata metadata)
    {
        System.out.println("create Person");
        System.out.println("Topic " + metadata.topic() + " partition: " + metadata.partition() + " Person: " + person.getName());
        System.out.println("--------------------------------------------------------------------");
    }

    @PersonCustomListener(groupId = "group-2")
    // @KafkaListener(topics = "person-topic",groupId = "group-2", containerFactory = "personKafkaListenerContainerFactory")
    public void historyPerson(Person person, ConsumerRecordMetadata metadata)
    {
        System.out.println("historic listeneer");
        System.out.println("Topic " + metadata.topic() + " partition: " + metadata.partition() + " Person: " + person.getName());
        System.out.println("--------------------------------------------------------------------");
    }


}
