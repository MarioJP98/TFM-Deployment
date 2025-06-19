#!/bin/sh

# Esperamos unos segundos a que el broker arranque
sleep 10

echo "Creando topic music-recommendation-features si no existe previamente"

kafka-topics --bootstrap-server broker:29092 \
  --create --if-not-exists \
  --topic music-recommendation-features \
  --partitions 1 \
  --replication-factor 1

echo "Creando topic music-recommendation-result si no existe previamente"
kafka-topics --bootstrap-server broker:29092 \
  --create --if-not-exists \
  --topic music-recommendation-result \
  --partitions 1 \
  --replication-factor 1

echo "Topics creados"
