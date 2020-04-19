package com.hackpro.kafkabeginners;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class ConsumerGroupsThread {

    private ConsumerGroupsThread() {
    }

    private void run() {
        String groupId = "my-fourth-application";
        String offsetOption = "earliest";

        // create properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.SERVER_CONFIG);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetOption);

        CountDownLatch countDownLatch = new CountDownLatch(1);
        // Invoke the ConsumerGroupRunnable
        ConsumerGroupRunnable consumerGroupRunnable = new ConsumerGroupRunnable(countDownLatch, properties);

        Thread myConsumerThread1 = new Thread(consumerGroupRunnable);
        // Start the thread
        myConsumerThread1.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.error("Shutdown hook initiated!!!");
            consumerGroupRunnable.shutdown();
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            log.info("Application Exited Successfully!!!");
        }));

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            log.error("Application closing!!!");
        } finally {
            log.info("Application Shutdown!!!");
        }
    }


    public static void main(String[] args) {
        new ConsumerGroupsThread().run();
    }


    public class ConsumerGroupRunnable implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> kafkaConsumer;

        public ConsumerGroupRunnable(CountDownLatch latch, Properties properties) {
            this.latch = latch;
            // create consumer
            this.kafkaConsumer = new KafkaConsumer<String, String>(properties);
            //Subscribe Consumer to certain topics
            kafkaConsumer.subscribe(Arrays.asList("second-topic"));
        }

        @Override
        public void run() {
            try {
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
            } catch (WakeupException ex) {
                log.error("Recieved Wakeup Signal!!!");
            } finally {
                // countdown the latch
                latch.countDown();
            }
        }

        public void shutdown() {
            // throws WakeupException and interrupt the thread
            kafkaConsumer.wakeup();
        }
    }
}
