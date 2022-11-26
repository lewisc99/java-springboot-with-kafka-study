package com.lewis.springbootwithkafka.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import java.time.LocalDateTime;

@RestController
public class ProducerController {
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

//    private int count;
    @GetMapping("send")
    public void send()
    {
          System.out.println(LocalDateTime.now());
          kafkaTemplate.send("topic-1","Olá Mundo" );
         //   count++;
    }

}
