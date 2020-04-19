package com.hackpro.kafkabeginners;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class ProducerCallback {

    public static void main(String[] args) {
        // create properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.SERVER_CONFIG);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // create Kafka
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {
            // create kafka records
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("second-topic", "Hackpro Teach team " + i);
            // send data
            kafkaProducer.send(producerRecord, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        log.info("Topic -> " + recordMetadata.topic() + "\n" +
                                "Partition -> " + recordMetadata.partition() + "\n" +
                                "Offset -> " + recordMetadata.offset() + "\n" +
                                "Timestamp -> " + recordMetadata.timestamp() + "\n");
                    } else {
                        log.error("Error while producing message", e);
                    }
                }
            });
        }
        kafkaProducer.close();
    }
}
