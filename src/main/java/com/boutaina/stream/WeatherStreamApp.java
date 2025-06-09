package com.boutaina.stream;
import com.boutaina.model.WeatherRecord;
import com.boutaina.serde.WeatherAggregatorSerde;
import com.boutaina.serde.WeatherRecordSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

public class WeatherStreamApp {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "weather-streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> input = builder.stream("weather-data");

        KStream<String, WeatherRecord> parsed = input
                .mapValues(WeatherRecord::fromCSV)
                .filter((key, value) -> value.temperature > 30)
                .mapValues(record -> {
                    double tempF = (record.temperature * 9 / 5) + 32;
                    return new WeatherRecord(record.station, tempF, record.humidity);
                });

        KGroupedStream<String, WeatherRecord> grouped = parsed
                .groupBy((key, value) -> value.station, Grouped.with(Serdes.String(), new WeatherRecordSerde()));

        KTable<String, WeatherRecord> averages = grouped
                .aggregate(
                        WeatherAggregator::new,
                        (key, value, aggregator) -> aggregator.add(value),
                        Materialized.with(Serdes.String(), new WeatherAggregatorSerde())
                )
                .mapValues(WeatherAggregator::computeAverage);

        averages.toStream()
                .mapValues(WeatherRecord::toCSV)
                .to("station-averages", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();
    }
}
