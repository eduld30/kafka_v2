package com.ucmmaster.kafka.simple;

import java.util.Random;

class TemperatureTelemetry {

    private static final Random RANDOM = new Random();

    /**
     * Device id
     */
    private final int id;
    /**
     * Temperature
     */
    private final int temperature;

    public TemperatureTelemetry(int id, int temperature) {
        this.id = id;
        this.temperature = temperature;
    }

    public int getId() {
        return id;
    }
    public int getTemperature() {
        return temperature;
    }

    @Override
    public String toString() {
        return String.format("{\"id\":%d, \"temperature\":%d}", id, temperature);
    }

    public static TemperatureTelemetry newRandomTemperatureRead(){
        int id = RANDOM.ints(1, 10).findFirst().getAsInt();
        int temperature = RANDOM.ints(15, 40).findFirst().getAsInt();
        return new TemperatureTelemetry(id,temperature);
    }
}
