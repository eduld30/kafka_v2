#/bin/bash

#datagen
docker compose exec connect confluent-hub install --no-prompt confluentinc/kafka-connect-datagen:latest

#jdbc
docker compose exec connect confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:latest

#cdc mysql
docker compose exec connect confluent-hub install --no-prompt debezium/debezium-connector-mysql:latest

#mongodb
docker compose exec connect confluent-hub install --no-prompt mongodb/kafka-connect-mongodb:latest

#adls
docker compose exec connect  confluent-hub install --no-prompt confluentinc/kafka-connect-azure-data-lake-gen2-storage:latest

#copia el driver jdbc mysql dentro del contenedor connect
docker cp ./mysql/mysql-connector-java-5.1.45.jar connect:/usr/share/confluent-hub-components/confluentinc-kafka-connect-jdbc/lib/mysql-connector-java-5.1.45.jar

docker compose restart connect

echo "Esperando 30s a que connect reinicie"

sleep 30

# lista todos los conector plugins disponibles
curl http://localhost:8083/connector-plugins | jq
