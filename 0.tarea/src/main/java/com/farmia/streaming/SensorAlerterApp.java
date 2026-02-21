package com.farmia.streaming;

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
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class SensorAlerterApp {

    private static Topology createTopology() {
        final String inputTopic = "sensor-telemetry";
        final String outputTopic = "sensor-alerts";

        //Creamos un Serde de tipo Avro, ya que el productor produce <String, SensorTelemetry>
        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", "http://localhost:8081");
        Serde<SensorTelemetry> sensorTelemetrySerde = new SpecificAvroSerde<>();
        sensorTelemetrySerde.configure(serdeConfig, false);

        //Creamos el KStream mediante el builder
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, SensorTelemetry> firstStream =
                builder.stream(inputTopic, Consumed.with(Serdes.String(), sensorTelemetrySerde));

        //Filtramos los eventos con temperatura >= 30 grados
        firstStream
                .peek((key, value) -> System.out.println("Incoming record - key " + key + " value " + value))
                .filter((key, value) -> value.getTemperature() >= 30)
                .peek((key, value) -> System.out.println("Outgoing record - key " + key + " value " + value))
                .to(outputTopic, Produced.with(Serdes.String(), sensorTelemetrySerde));

        return builder.build();
    }

    public static void main(String[] args) throws IOException {

        // Cargamos la configuración
        Properties props = ConfigLoader.getProperties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kstream-app");

        // Creamos la topología
        Topology topology = createTopology();

        KafkaStreams streams = new KafkaStreams(topology, props);
        // Iniciar Kafka Streams
        streams.start();
        // Parada controlada en caso de apagado
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}