package com.twitter.record;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class RecordDeserializer implements Deserializer {
    @Override
    public void configure(Map map, boolean b) {

    }

    @Override
    public Object deserialize(String s, byte[] bytes) {
        ObjectMapper mapper = new ObjectMapper();
        MyTwitterRecord record = null;
        try {
            record = mapper.readValue(bytes, MyTwitterRecord.class);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return record;
    }

    @Override
    public void close() {

    }
}
