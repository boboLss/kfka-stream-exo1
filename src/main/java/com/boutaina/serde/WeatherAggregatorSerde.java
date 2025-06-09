package com.boutaina.serde;
import com.boutaina.stream.WeatherAggregator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.*;

import java.util.Map;

public class WeatherAggregatorSerde implements Serde<WeatherAggregator> {
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public Serializer<WeatherAggregator> serializer() {
        return new Serializer<WeatherAggregator>() {
            @Override
            public byte[] serialize(String topic, WeatherAggregator data) {
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
    public Deserializer<WeatherAggregator> deserializer() {
        return new Deserializer<WeatherAggregator>() {
            @Override
            public WeatherAggregator deserialize(String topic, byte[] bytes) {
                try {
                    return mapper.readValue(bytes, WeatherAggregator.class);
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
