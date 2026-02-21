package com.farmia.streaming;

import com.farmia.sales.SalesSummary;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import javax.swing.*;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class SalesSummaryApp {

    private static Topology createTopology() {
        final String inputTopic = "sales_transactions";
        final String outputTopic = "sales-summary";

        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", "http://localhost:8081");

        // Input avro SalesTransaction
        Serde<GenericRecord> txSerde = new GenericAvroSerde();
        txSerde.configure(serdeConfig, false);

        // Output avro SensorAlerts
        Serde<SalesSummary> salesSummarySerde = new SpecificAvroSerde<>();
        salesSummarySerde.configure(serdeConfig, false);

        // Creamos el KStream mediante el builder
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, GenericRecord> summaryStream =
                builder.stream(inputTopic, Consumed.with(Serdes.String(), txSerde));

        // Agrupamos eventos por categoría para obtener el total de unidades vendidas y ganancias cada minuto
        summaryStream
                .peek((k, v) -> System.out.println("Incoming record - key " + k + " value " + v))

                // agrupamos por categoría en ventanas de 1 minuto
                .selectKey((k, v) -> v.get("category").toString())
                .groupByKey(Grouped.with(Serdes.String(), txSerde))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))

                // agregamos para obtener cantidad de productos y ganancias totales
                .aggregate(
                        // valor inicial
                        () -> SalesSummary.newBuilder()
                                .setCategory("")
                                .setTotalQuantity(0)
                                .setTotalRevenue(0.0F)
                                .setWindowStart(Instant.ofEpochSecond(0L))
                                .setWindowEnd(Instant.ofEpochSecond(0L))
                                .build(),
                        // agregador
                        (category, tx, agg) -> {
                            int quantity = (Integer) tx.get("quantity");

                            // convertir price a float (jdbc connector lo serializa como bytes)
                            ByteBuffer bb = (ByteBuffer) tx.get("price");
                            Schema priceSchema = tx.getSchema().getField("price").schema();
                            BigDecimal bd = new Conversions.DecimalConversion()
                                    .fromBytes(bb, priceSchema, priceSchema.getLogicalType());
                            float price = bd.floatValue();

                            return SalesSummary.newBuilder(agg)
                                    .setCategory(category)
                                    .setTotalQuantity(agg.getTotalQuantity() + quantity)
                                    .setTotalRevenue(agg.getTotalRevenue() + (quantity * price))
                                    .build();
                        },
                        Materialized.with(Serdes.String(), salesSummarySerde)
                )
                .toStream()

                // construcción y producción del objeto final SalesSummary
                .map((wKey, agg) -> KeyValue.pair(
                        wKey.key(),
                        SalesSummary.newBuilder(agg)
                                .setWindowStart(wKey.window().startTime())
                                .setWindowEnd(wKey.window().endTime())
                                .build()

                ))
                .peek((k, v) -> System.out.println("Outgoing summary - key " + k + " value " + v))
                .to(outputTopic, Produced.with(Serdes.String(), salesSummarySerde));

        return builder.build();
    }

    public static void main(String[] args) throws IOException {

        // Cargamos la configuración
        Properties props = ConfigLoader.getProperties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "sales-summary-app");

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