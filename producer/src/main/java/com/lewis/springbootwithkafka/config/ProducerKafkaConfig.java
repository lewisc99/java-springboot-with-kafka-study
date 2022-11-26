package com.lewis.springbootwithkafka.config;


import com.fasterxml.jackson.databind.JsonSerializable;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Objects;

@Configuration
public class ProducerKafkaConfig {

    @Autowired
    private KafkaProperties kafkaProperties;

    @Bean
    public ProducerFactory<String,String> producerFactory()
    {
        var configs = new HashMap<String, Object>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers()); //will get the host of kafka
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        return new DefaultKafkaProducerFactory<>(configs);
    }
    @Bean
    public KafkaTemplate<String,String> kafkaTemplate()
    {
        return  new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public ProducerFactory<String,Object> jsonProducerFactory()
    {
        var configs = new HashMap<String, Object>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers()); //will get the host of kafka
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializable.class);

        return new DefaultKafkaProducerFactory<>(configs,new StringSerializer(), new JsonSerializer<>());
    }
    @Bean
    public KafkaTemplate<String, Serializable> JsonkafkaTemplate()
    {
        return  new KafkaTemplate(jsonProducerFactory());
    }

    @Bean
    public KafkaAdmin kafkaAdmin()
    {
        var configs = new HashMap<String,Object>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());

        return new KafkaAdmin(configs);
    }
//    @Bean
//    public NewTopic topic1()
//    {
//        //String name, int number of Partitions, short replicationFactor
//        return new NewTopic("topic-1", 2, Short.valueOf("1"));
//
//        //kafka version 2.6
//      //  return TopicBuilder.name("topic-1").build();
//    }

   // version kafka 2.7
    @Bean
    public KafkaAdmin.NewTopics topics()
    {
        return new KafkaAdmin.NewTopics(

                TopicBuilder.name("topic-1").partitions(2).replicas(1).build(),
                TopicBuilder.name("person-topic").partitions(2).build()
        );
    }



}
