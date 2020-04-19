package com.hackpro.kafkabeginners;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

@Slf4j
public class ConsumerAssignSeek {
    public static void main(String[] args) {
        String offsetOption = "earliest";
        String topic = "second-topic";

        // create properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.SERVER_CONFIG);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetOption);

        // create consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        // Assign and Seek used for Replay option to fetch particular message
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        kafkaConsumer.assign(Arrays.asList(partitionToReadFrom));

        // Seek to set the consumer to read from particular offset
        long offsetReadFrom = 10L;
        kafkaConsumer.seek(partitionToReadFrom, offsetReadFrom);

        int numberOfMessagesToRead = 10;
        int numberOfMessagesReadSoFar = 0;
        boolean keepOnReading = true;

        // poll records
        while (keepOnReading) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> records : consumerRecords) {
                numberOfMessagesReadSoFar++;
                log.info("topic -> " + records.topic() + "\n" +
                        "key -> " + records.key() + "\n" +
                        "value -> " + records.value() + "\n" +
                        "partition -> " + records.partition() + "\n" +
                        "offset -> " + records.offset());
            }
            if (numberOfMessagesReadSoFar > numberOfMessagesToRead) {
                log.info("Message read successfully!!!");
                keepOnReading = false;
                break;
            }
        }
        log.info("Application Exited!!!");
    }
}
