package com.boutaina.stream;

import com.boutaina.model.WeatherRecord;

public class WeatherAggregator {
    private double totalTemp;
    private double totalHumidity;
    private long count;

    public WeatherAggregator() {
        this.totalTemp = 0;
        this.totalHumidity = 0;
        this.count = 0;
    }

    public WeatherAggregator add(WeatherRecord record) {
        this.totalTemp += record.temperature;
        this.totalHumidity += record.humidity;
        this.count += 1;
        return this;
    }

    public WeatherRecord computeAverage() {
        if (count == 0) return new WeatherRecord("unknown", 0, 0);
        return new WeatherRecord("average", totalTemp / count, totalHumidity / count);
    }

    // Getters and setters (si nécessaire pour Jackson)
    public double getTotalTemp() { return totalTemp; }
    public double getTotalHumidity() { return totalHumidity; }
    public long getCount() { return count; }

    public void setTotalTemp(double totalTemp) { this.totalTemp = totalTemp; }
    public void setTotalHumidity(double totalHumidity) { this.totalHumidity = totalHumidity; }
    public void setCount(long count) { this.count = count; }
}
