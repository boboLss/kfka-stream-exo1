package com.boutaina.serde;
import com.boutaina.model.WeatherRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.*;

import java.util.Map;

public class WeatherRecordSerde implements Serde<WeatherRecord> {
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public Serializer<WeatherRecord> serializer() {
        return new Serializer<WeatherRecord>() {
            @Override
            public byte[] serialize(String topic, WeatherRecord data) {
                try {
                    return mapper.writeValueAsBytes(data);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {}
            @Override
            public void close() {}
        };
    }

    @Override
    public Deserializer<WeatherRecord> deserializer() {
        return new Deserializer<WeatherRecord>() {
            @Override
            public WeatherRecord deserialize(String topic, byte[] bytes) {
                try {
                    return mapper.readValue(bytes, WeatherRecord.class);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {}
            @Override
            public void close() {}
        };
    }
}
