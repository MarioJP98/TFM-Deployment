# Usa Debian 12 (bookworm) explícitamente
FROM python:3.11-slim-bookworm

# Instala Java 17 (headless para aligerar), curl y certificados
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Spark 3.5.1 precompilado para Hadoop3
ENV SPARK_VERSION=3.5.1
ENV SPARK_PACKAGE=spark-${SPARK_VERSION}-bin-hadoop3
RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_PACKAGE}.tgz \
    | tar -xz -C /opt

ENV SPARK_HOME=/opt/${SPARK_PACKAGE}
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# Python deps
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

WORKDIR /app
EXPOSE 8888
