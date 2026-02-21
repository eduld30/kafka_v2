package com.farmia.streaming;

import com.farmia.iot.AlertType;
import com.farmia.iot.SensorAlert;
import com.farmia.iot.SensorTelemetry;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class SensorAlerterApp {

    private static Topology createTopology() {
        final String inputTopic = "sensor-telemetry";
        final String outputTopic = "sensor-alerts";

        final double TEMP_THRESHOLD = 35.0;
        final double HUM_THRESHOLD = 20.0;

        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", "http://localhost:8081");

        // Input avro SensorTelemetry
        Serde<SensorTelemetry> sensorTelemetrySerde = new SpecificAvroSerde<>();
        sensorTelemetrySerde.configure(serdeConfig, false);

        // Output avro SensorAlerts
        Serde<SensorAlert> sensorAlertSerde = new SpecificAvroSerde<>();
        sensorAlertSerde.configure(serdeConfig, false);

        // Creamos el KStream mediante el builder
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, SensorTelemetry> alertStream =
                builder.stream(inputTopic, Consumed.with(Serdes.String(), sensorTelemetrySerde));

        // Filtramos los eventos con temperatura > 35 o humedad < 20 y lanzamos alerta
        alertStream
                .peek((k, v) -> System.out.println("Incoming record - key " + k + " value " + v))
                .filter((k, v) ->
                        v != null && (v.getTemperature() > TEMP_THRESHOLD || v.getHumidity() < HUM_THRESHOLD)
                )
                .mapValues(v -> {
                    final String sensorId = v.getSensorId().toString();
                    final Instant ts = v.getTimestamp();

                    final boolean highTemp = v.getTemperature() > TEMP_THRESHOLD;
                    final boolean lowHum = v.getHumidity() < HUM_THRESHOLD;

                    final AlertType type;
                    final String details;

                    if (highTemp && lowHum) {
                        type = AlertType.HIGH_TEMPERATURE_AND_LOW_HUMIDITY;
                        details = "Temperature exceeded 35ºC; Humidity below 20%";
                    } else if (highTemp) {
                        type = AlertType.HIGH_TEMPERATURE;
                        details = "Temperature exceeded 35ºC";
                    } else {
                        type = AlertType.LOW_HUMIDITY;
                        details = "Humidity below 20%";
                    }

                    return SensorAlert.newBuilder()
                            .setSensorId(sensorId)
                            .setAlertType(type)
                            .setTimestamp(ts)
                            .setDetails(details)
                            .build();
                })
                .peek((k, v) -> System.out.println("Outgoing alert - key " + k + " value " + v))
                .to(outputTopic, Produced.with(Serdes.String(), sensorAlertSerde));

        return builder.build();
    }

    public static void main(String[] args) throws IOException {

        // Cargamos la configuración
        Properties props = ConfigLoader.getProperties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "sensor-alerter-app");

        // Creamos la topología
        Topology topology = createTopology();

        KafkaStreams streams = new KafkaStreams(topology, props);

        // Ver excepciones
        streams.setUncaughtExceptionHandler((t, e) -> {
            System.err.println("Uncaught exception in thread " + t.getName());
            e.printStackTrace();
        });

        // Iniciar Kafka Streams
        streams.start();

        // Parada controlada en caso de apagado
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}