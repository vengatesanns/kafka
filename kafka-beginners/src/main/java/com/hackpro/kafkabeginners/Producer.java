package com.hackpro.kafkabeginners;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer {
    public static void main(String[] args) {
        // create properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.SERVER_CONFIG);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // create Kafka
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        // create kafka records
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("second-topic", "Hi, Welcome to HackPro Tech Tech Tech Tech Tech!!!!!!!!!");
        // send data
        kafkaProducer.send(producerRecord);
        kafkaProducer.close();
    }
}
