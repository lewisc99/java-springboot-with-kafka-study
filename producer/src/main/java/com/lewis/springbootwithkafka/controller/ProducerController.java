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

    @GetMapping("send")
    public ResponseEntity<String> send()
    {
        kafkaTemplate.send("topic-1","Envio de: " + LocalDateTime.now());

        return ResponseEntity.ok().build();
    }
}
