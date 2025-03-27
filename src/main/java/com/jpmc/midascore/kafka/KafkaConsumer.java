package com.jpmc.midascore.kafka;

import com.jpmc.midascore.foundation.Transaction;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class KafkaConsumer {


    @KafkaListener(topics = "${general.kafka-topic}",groupId = "midas-core-groupId")
    public void consumer(ConsumerRecord<String, Transaction> record){

        Transaction transaction = record.value();

        System.out.println("Kafka Consumer Received Message: " + transaction);

    }


}
