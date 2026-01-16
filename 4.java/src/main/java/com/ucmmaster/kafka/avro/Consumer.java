package com.ucmmaster.kafka.avro;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Properties;

import static java.util.Collections.singleton;

import com.ucmmaster.kafka.data.v1.TemperatureTelemetry;

class Consumer {

    private final Logger logger = LoggerFactory.getLogger(Consumer.class.getName());

    private final String name;
    private final KafkaConsumer<String, TemperatureTelemetry> consumer;

    public Consumer(String name, String config) {
        this.name = name;
        this.consumer = createConsumer(config);
    }

    private KafkaConsumer<String, TemperatureTelemetry> createConsumer(String config) {
        Properties properties = new Properties();
        try (InputStream input = Consumer.class.getClassLoader().getResourceAsStream(config)) {
            properties.load(input);
            return new KafkaConsumer<>(properties);
        } catch (IOException ex) {
            logger.error(ex.getMessage());
        }
        return null;

    }

    public void consume(String topic) {
        try {
            consumer.subscribe(singleton(topic));
            while (true) {
                ConsumerRecords<String, TemperatureTelemetry> records = consumer.poll(Duration.ofMillis(100));
                records.forEach(record -> {
                    logger.info("[{}] record consumed partition:{} offset:{} timestamp:{} key:{} value:{} ", this.name, record.partition(), record.offset(), record.timestamp(), record.key(), record.value());
                });
            }
        } catch (Exception ex) {
            logger.error(ex.getMessage());
        } finally {
            consumer.close();
            logger.error("Consumer closed.");
        }
    }

}
