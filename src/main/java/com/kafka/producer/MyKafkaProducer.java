package com.kafka.producer;
import java.util.Properties;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongSerializer;

public class MyKafkaProducer {

    private final static String KAFKA_TOPIC = "data";
    private static final String KAFKA_SERVERS = "localhost:9092";
    private static final Integer BUFFER_MEMORY = 33554432;
    private static final Integer BATCH_SIZE = 16384;
    private Producer<Long, byte[]> kafkaProducer;

    public MyKafkaProducer() {
        /* Constructor */
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

    public void sendMessageToKafkaBroker(byte[] message) throws Exception {
        /* Function to send Message to Kafka - Message has the current time as the index and the bytearray of twitter record */

        long time = System.currentTimeMillis();

        try {
            final ProducerRecord<Long, byte[]> record = new ProducerRecord<Long, byte[]>(KAFKA_TOPIC, time, message);

            RecordMetadata metadata =  this.kafkaProducer.send(record).get();

            long elapsedTime = System.currentTimeMillis() - time;
            System.out.printf("sent record(key=%s value=%s) " +
                            "meta(partition=%d, offset=%d) time=%d\n",
                    record.key(), record.value(), metadata.partition(),
                    metadata.offset(), elapsedTime);
        }
        finally {
            System.out.println("SUCCESS");
        }
    }

    public void stopKafkaProducer() throws Exception {
        /* function to stop Kafka Producer */
        this.kafkaProducer.flush();
        this.kafkaProducer.close();
    }

}
