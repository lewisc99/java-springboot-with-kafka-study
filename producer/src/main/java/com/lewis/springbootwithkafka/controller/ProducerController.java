package com.lewis.springbootwithkafka.controller;

import com.lewis.springbootwithkafka.models.Person;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.Serializable;
import java.time.LocalDateTime;

@RestController
public class ProducerController {
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private KafkaTemplate<String, Serializable> jsonKafkaTemplate;

//    private int count;
    @GetMapping("send")
    public void send()
    {
          System.out.println(LocalDateTime.now());
          kafkaTemplate.send("topic-1","Olá Mundo" );
         //   count++;
    }
    @GetMapping("send-person")
    public void sendPerson()
    {
        System.out.println(LocalDateTime.now());
        jsonKafkaTemplate.send("person-topic", new Person("Joao")  );

    }


}
