FROM bitnami/spark:3.5.1

USER root

RUN apt-get update && apt-get install -y python3-pip

# Instalamos pip para pyspark + kafka dependencies (opcional si no están)
RUN pip3 install numpy pandas kafka-python

# Copiamos el consumer y el modelo dentro del contenedor (Esta comentado por montarlo como volumen)
# COPY spark-consumer/ /app/
# COPY spark-models/ /app/models/

# Ejecutamos directamente el spark-submit desde dentro
ENTRYPOINT [ "/opt/bitnami/spark/bin/spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1", "/app/consumer.py" ]
