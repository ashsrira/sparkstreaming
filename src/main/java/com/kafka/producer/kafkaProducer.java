package com.kafka.producer;

import java.util.Properties;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class kafkaProducer {

    private final static String KAFKA_TOPIC = "data";
    private static final String KAFKA_SERVERS = "localhost:9092";
    private static final Integer BUFFER_MEMORY = 33554432;
    private static final Integer BATCH_SIZE = 16384;
    private Producer<String, String> kafkaProducer;

    public void createProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers",KAFKA_SERVERS);
        props.put("acks","all");
        props.put("retries", 0);
        props.put("batch.size", BATCH_SIZE);
        props.put("linger.ms", 1);
        props.put("buffer.memory", BUFFER_MEMORY);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        this.kafkaProducer = new KafkaProducer<>(props);
    }
}
