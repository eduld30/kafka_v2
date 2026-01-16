# CLI (Command Line Interface)

## Objetivo

Asimilar los conceptos mediante el uso de distintas herramientas de consola disponibles por defecto en cualquier distribución **Kafka**

## Práctica

Lo primero que tenemos que hacer es habilitar una **consola interactiva** dentro de uno de los contenedores brokers de nuestro cluster

```bash
docker exec -it broker-1 /bin/bash
```

## Admin API

### kafka-configs

El comando **kafka-configs** permite cambiar la configuración de los brokers y los topics de un cluster

[Kafka Broker-Level Config](http://kafka.apache.org/10/documentation.html#brokerconfigs)

[Kafka Topic-Level Configs](http://kafka.apache.org/10/documentation.html#topicconfigs)

#### Lista propiedades de configuración a nivel de cluster

```bash
kafka-configs --bootstrap-server broker-1:29092 --entity-type brokers --describe --all
```

#### Cambiar propiedades de configuración a nivel de cluster

En este caso, vamos a cambiar el tamaño máximo de los segmentos (log.segment.bytes):

```bash
kafka-configs --bootstrap-server broker-1:29092 --entity-type brokers --entity-default --alter --add-config log.segment.bytes=10485760
```

Verifica que se ha cambiado

```bash
kafka-configs --bootstrap-server broker-1:29092 --entity-type brokers --describe --all | grep log.segment.bytes
```

Restauramos la configuración previa

```bash
kafka-configs --bootstrap-server broker-1:29092 --entity-type brokers --entity-default --alter --add-config log.segment.bytes=1073741824
```

### kafka-topics

El comando **kafka-topics** permite crear y administrar los topics de un cluster.

#### Listar todos los topics

```bash
kafka-topics --bootstrap-server broker-1:29092 --list
```

#### Creación de un topic

En este caso, vamos a crear un topic llamado **my-topic** con 4 particiones, factor de replicación 2 y personalizando un par de propiedades (message.max.bytes y flush.messages=1)

```bash
kafka-topics --bootstrap-server broker-1:29092 --create --topic my-topic --partitions 4 --replication-factor 2 --config max.message.bytes=64000 --config flush.messages=1
```

#### Describir un topic

```bash
kafka-topics --bootstrap-server broker-1:29092 --topic my-topic --describe
```

#### Modificación de configuración de un topic

```bash
kafka-configs --bootstrap-server broker-1:29092 --entity-type topics --entity-name my-topic --alter --add-config max.message.bytes=128000
```

#### Modificación del número de particiones de un topic

```bash
kafka-topics --bootstrap-server broker-1:29092 --alter --topic my-topic --partitions 6
```

#### Borrar un topic

```bash
kafka-topics --bootstrap-server broker-1:29092 --delete --topic my-topic
```

### Control Center

Trata de explorar la interfaz web de Control Center para hacer realizar operaciones anteriores

## Producer / Consumer API

### kafka-console-producer

El comando **kafka-console-producer** nos permite producir datos en un topic.

#### Producir datos en un topic

Primero crearemos un topic **temperature-telemetry** con 3 particiones y factor de réplica 3.

```bash
kafka-topics --bootstrap-server broker-1:29092 --create --topic temperature-telemetry --partitions 3 --replication-factor 3 --config flush.messages=1
```

Lo siguiente ejecutar el comando para producir

```bash
kafka-console-producer --bootstrap-server broker-1:29092 --topic temperature-telemetry --property "parse.key=true" --property "key.separator=,"
```

Este comando es interactivo, y producirá un mensaje por cada línea que escribamos

Los mensajes a producir seguirán el siguiente formato: **key**,**value**

Es decir lo que va antes de la coma es la clave (key) y lo que va después es el valor (value). En este caso el value de nuestros mensajes es un objeto JSON

```json
1,{"id": 1, "temperature": 15}
2,{"id": 2, "temperature": 20}
1,{"id": 1, "temperature": 16}
3,{"id": 3, "temperature": 18}
4,{"id": 4, "temperature": 18}
5,{"id": 5, "temperature": 17}
4,{"id": 4, "temperature": 21}
```

Copia de una en una cada línea, pégala en la consola interactiva del producer y pulsa intro

### kafka-console-consumer

El comando **kafka-console-consumer** nos permite consumir datos en un topic.

#### Consumir datos en un topic

Ahora crearemos un consumidor de consola para consumir los datos

```bash
kafka-console-consumer --bootstrap-server broker-1:29092 --topic temperature-telemetry --property print.key=true --from-beginning
```

¿Qué pasa cuando este arranca?

<details>
  <summary><b>Solución</b></summary>

¡El consumidor consume todos los mensajes!.
</details>

¿Que pasara si añadimos otro consumidor?

<details>
  <summary><b>Solución</b></summary>

¡Tenemos dos consumidores consumiendo exactamente los mismos mensajes!.
</details>

#### Consumir datos en un topic con un grupo de consumo

Ahora consumiremos con dos consumidores formando un único grupo de consumo (**console-group**):

```bash
kafka-console-consumer --bootstrap-server broker-1:29092 --topic temperature-telemetry --property print.key=true --group console-group
```

Observad el rebalanceo y particionado que se produce mediante la partition key elegida.

> ❗️ **NOTA**<br/>Para detener una aplicación de consola debemos pulsar **Ctrl+C**