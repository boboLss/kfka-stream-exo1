package com.boutaina.model;

public class WeatherRecord {
    public String station;
    public double temperature;
    public double humidity;

    public WeatherRecord() {}

    public WeatherRecord(String station, double temperature, double humidity) {
        this.station = station;
        this.temperature = temperature;
        this.humidity = humidity;
    }

    public static WeatherRecord fromCSV(String csvLine) {
        String[] parts = csvLine.split(",");
        return new WeatherRecord(parts[0], Double.parseDouble(parts[1]), Double.parseDouble(parts[2]));
    }

    public String toCSV() {
        return station + "," + temperature + "," + humidity;
    }
}

