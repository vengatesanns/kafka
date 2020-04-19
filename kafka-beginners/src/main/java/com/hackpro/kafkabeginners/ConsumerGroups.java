package com.hackpro.kafkabeginners;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

@Slf4j
public class ConsumerGroups {
    public static void main(String[] args) {


        String groupid = "my-third-application";
        String offsetOption = "earliest";

        // create properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.SERVER_CONFIG);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupid);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetOption);

        // create consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        //Subscribe Consumer to certain topics
        kafkaConsumer.subscribe(Arrays.asList("second-topic"));

        // poll records
        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> records : consumerRecords) {
                log.info("topic -> " + records.topic() + "\n" +
                                "key -> " + records.key() + "\n" +
                                "value -> " + records.value() + "\n" +
                                "partition -> " + records.partition() + "\n" +
                        "offset -> " + records.offset());
            }
        }
    }
}
