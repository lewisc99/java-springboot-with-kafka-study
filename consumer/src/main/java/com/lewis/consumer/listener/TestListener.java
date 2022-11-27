package com.lewis.consumer.listener;

import com.lewis.consumer.custom.PersonCustomListener;
import com.lewis.consumer.models.City;
import com.lewis.consumer.models.Person;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.listener.adapter.ConsumerRecordMetadata;

import java.util.List;

@Configuration
public class TestListener {

    //encapsulate ConsumerRecordMetadata metadata only available kafka -v 2.5
    @KafkaListener(topics = "topic-1",groupId = "group-1")
    public void listen(String message, ConsumerRecordMetadata metadata)
    {
     System.out.println("Topic " + metadata.topic() + " partition: " + metadata.partition() + " Message: " + message +
             " Offset: " + metadata.offset() + " Thread: "+ Thread.currentThread().getId());
     System.out.println("--------------------------------------------------------------------");

    }

    //messaging in lot string (list of messages)
//    @KafkaListener(topics = "topic-1",groupId = "group-1")
//    public void listen(List<String> message)
//    {
//        System.out.println(message);
//        System.out.println("--------------------------------------------------------------------");
//
//    }

   // @KafkaListener(topics = "person-topic",groupId = "group-1", containerFactory = "personKafkaListenerContainerFactory")
    @PersonCustomListener(groupId = "group-1")
    public void createPerson(Person person, ConsumerRecordMetadata metadata)
    {
        System.out.println("create Person");
        System.out.println("Topic " + metadata.topic() + " partition: " + metadata.partition() + " Person: " + person.getName()
                + " Thread: "+ Thread.currentThread().getId());
        System.out.println("--------------------------------------------------------------------");
    }

    @PersonCustomListener(groupId = "group-2")
    // @KafkaListener(topics = "person-topic",groupId = "group-2", containerFactory = "personKafkaListenerContainerFactory")
    public void historyPerson(Person person, ConsumerRecordMetadata metadata)
    {

        System.out.println("historic listeneer");
        System.out.println("Topic " + metadata.topic() + " partition: " + metadata.partition() + " Person: " + person.getName()
                + " Thread: "+ Thread.currentThread().getId());
        System.out.println("--------------------------------------------------------------------");

      //  throw new IllegalArgumentException("Fail listener");

    }


    //0-5 or 0-5,9, 0,9
    @KafkaListener(groupId = "my-group", topicPartitions = {@TopicPartition(topic = "my-topic", partitions = "0")})
    public void listenPartition(String message, ConsumerRecordMetadata metadata)
    {
        System.out.println(" partition 0: " + metadata.partition() + " Message: " + message );
        System.out.println("----------------------------------------------------------------------------");

    }

    @KafkaListener(groupId = "my-group", topicPartitions = {@TopicPartition(topic = "my-topic", partitions = "1-9")})
    public void listenPartition2(String message, ConsumerRecordMetadata metadata)
    {
        System.out.println(" partition 1-9: " + metadata.partition() + " Message: " + message );
        System.out.println("----------------------------------------------------------------------------");

    }

    @KafkaListener(groupId = "group-1", topics = "city-topic", containerFactory = "jsonkafkaListenerContainerFactory")
    public void createCity(City city, ConsumerRecordMetadata metadata)
    {
        System.out.println(" partition 1-9: " + metadata.partition() + " City: " + city.getName() + " UF: "+ city.getUF() );
        System.out.println("----------------------------------------------------------------------------");
    }



}
