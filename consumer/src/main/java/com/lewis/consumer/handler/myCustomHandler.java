package com.lewis.consumer.handler;


import org.springframework.kafka.listener.KafkaListenerErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;


@Component
public class myCustomHandler implements KafkaListenerErrorHandler {


    @Override
    public Object handleError(Message<?> message, ListenerExecutionFailedException exception) {

        System.out.println("Enter in the handler");
        System.out.println(message.getPayload());
        System.out.println(exception.getGroupId());

       // return null; //if returns null the application thinks that the exception was resolved
        //and not was thrown any error

        throw exception;
    }


}
