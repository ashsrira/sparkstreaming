package com.kafka.producer;

import java.io.ByteArrayOutputStream;
import java.util.Properties;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongSerializer;

public class kafkaProducer {

    private final static String KAFKA_TOPIC = "data";
    private static final String KAFKA_SERVERS = "localhost:9092";
    private static final Integer BUFFER_MEMORY = 33554432;
    private static final Integer BATCH_SIZE = 16384;
    private Producer<Long, byte[]> kafkaProducer;

    public kafkaProducer() {
        /* function to create Producer with the required Properties */
        Properties props = new Properties();
        props.put("bootstrap.servers",KAFKA_SERVERS);
        props.put("acks","all");
        props.put("retries", 0);
        props.put("batch.size", BATCH_SIZE);
        props.put("linger.ms", 1);
        props.put("buffer.memory", BUFFER_MEMORY);
        props.put("key.serializer", LongSerializer.class.getName());
        props.put("value.serializer", ByteArraySerializer.class.getName());
        this.kafkaProducer = new KafkaProducer<Long, byte[]>(props);

    }

//     public void sendMessage(String message) throws Exception {
//
//        long time = System.currentTimeMillis();
//
//        try {
//            //for (long index = time; index < time + sendMessageCount; index++) {
//            final ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(KAFKA_TOPIC, time, message);
//
//                RecordMetadata metadata =  this.kafkaProducer.send(record).get();
//
//                long elapsedTime = System.currentTimeMillis() - time;
//                System.out.printf("sent record(key=%s value=%s) " +
//                                "meta(partition=%d, offset=%d) time=%d\n",
//                        record.key(), record.value(), metadata.partition(),
//                        metadata.offset(), elapsedTime);
//
//            //}
//        } finally {
//                System.out.println("SUCCESS");
//        }
//    }

    public void sendMessage2(byte[] message) throws Exception {

        long time = System.currentTimeMillis();

        try {
            //for (long index = time; index < time + sendMessageCount; index++) {
            final ProducerRecord<Long, byte[]> record = new ProducerRecord<Long, byte[]>(KAFKA_TOPIC, time, message);

            RecordMetadata metadata =  this.kafkaProducer.send(record).get();

            long elapsedTime = System.currentTimeMillis() - time;
            System.out.printf("sent record(key=%s value=%s) " +
                            "meta(partition=%d, offset=%d) time=%d\n",
                    record.key(), record.value(), metadata.partition(),
                    metadata.offset(), elapsedTime);

            //}
        }
        catch (Exception e) {
            System.out.println(e.toString());
        }
            finally {
            System.out.println("SUCCESS");
        }
    }

    public void closeProducer() {
        this.kafkaProducer.flush();
        this.kafkaProducer.close();
    }

}
