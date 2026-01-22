#/bin/bash

cd ../1.environment

echo "Deteniendo entorno"
docker compose down --remove-orphans
echo "OK"
