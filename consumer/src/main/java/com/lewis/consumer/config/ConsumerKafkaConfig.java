package com.lewis.consumer.config;


import com.fasterxml.jackson.databind.JsonSerializable;
import com.lewis.consumer.models.Person;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RecordInterceptor;
import org.springframework.kafka.support.converter.JsonMessageConverter;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;

@EnableKafka
@Configuration
public class ConsumerKafkaConfig {

    @Autowired
    private KafkaProperties kafkaProperties;

    @Bean
    public ConsumerFactory<String,String> consumerFactory()
    {
        var configs = new HashMap<String, Object>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers()); //will get the host of kafka
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        return new DefaultKafkaConsumerFactory<>(configs);
    }


    //manager the listeners
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String,String> kafkaListenerContainerFactory()
    {
        var factory = new ConcurrentKafkaListenerContainerFactory<String,String>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(2);
     //  factory.setBatchListener(true); // send messages in list (lote).
        return factory;
    }


    @Bean
    public ConsumerFactory jsonConsumerFactory()
    {
        var configs = new HashMap<String, Object>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers()); //will get the host of kafka
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        return new DefaultKafkaConsumerFactory<>(configs);
    }


    //manager the listeners
    @Bean
    public ConcurrentKafkaListenerContainerFactory jsonkafkaListenerContainerFactory()
    {
        var factory = new ConcurrentKafkaListenerContainerFactory();
        factory.setConsumerFactory(jsonConsumerFactory());
        factory.setMessageConverter(new JsonMessageConverter());
        return factory;
    }



    @Bean
    public ConsumerFactory<String, Person> personConsumerFactory()
    {
        var configs = new HashMap<String, Object>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers()); //will get the host of kafka
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        var jsonDeserializer = new JsonDeserializer<>(Person.class)
                .trustedPackages("*") // * means to  trust in all packages that get from the publisher
                .forKeys(); //for keys the consumer class "Person" won't be necessary have the same package as the producer like com.consumer.models


        return new DefaultKafkaConsumerFactory(configs, new StringDeserializer(),jsonDeserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String,Person> personKafkaListenerContainerFactory()
    {
        var factory = new ConcurrentKafkaListenerContainerFactory<String,Person>();

        factory.setConsumerFactory(personConsumerFactory());
        factory.setCommonErrorHandler(defaultErrorHandler());
       // factory.setRecordInterceptor(adultInterceptor());
//          factory.setRecordInterceptor(exampleInterceptor());
        return factory;
    }
    //default is 1 try and then 9 try
    //custom 1 try in 1 sec, and maxAttemps 2 try
    private CommonErrorHandler defaultErrorHandler() {
        // return new DefaultErrorHandler(new FixedBackOff(100l,9)); default
        var recoverer = new DeadLetterPublishingRecoverer(new KafkaTemplate<>(deadLetterFactory()));
        return new DefaultErrorHandler(recoverer,new FixedBackOff(100l,2));
    }


    public ProducerFactory<String, Person> deadLetterFactory()
    {
        var configs = new HashMap<String, Object>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers()); //will get the host of kafka
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializable.class);

        return new DefaultKafkaProducerFactory<>(configs,new StringSerializer(), new JsonSerializer<>());
    }



    private RecordInterceptor<String, Person> exampleInterceptor() {

        return new RecordInterceptor<String, Person>() {
            @Override
            public ConsumerRecord<String, Person> intercept(ConsumerRecord<String, Person> consumerRecord) {
                return consumerRecord;
            }

            @Override
            public void success(final ConsumerRecord<String,Person> record, final Consumer<String,Person> consumer)
            {
              System.out.println("Succcess");
            }
            //you can implement another method similar to this intercept

            @Override
            public void failure(final ConsumerRecord<String,Person> record, final Exception exception,final Consumer<String,Person> consumer)
            {
                System.out.println("Fail");
            }
        };

    }

    private RecordInterceptor<String, Person> adultInterceptor() {
        return  record ->
        {
            System.out.println("Record Person Name : "+ record.value().getName());
             Person person = record.value();
            return person.getName() == "Joao" ? record : null;
           // return person.getAge() >= 18 ? record : null;
        };
    }


}
