FROM python:3.11-slim

RUN apt-get update && apt-get install -y openjdk-17-jdk curl ca-certificates && apt-get clean

ENV SPARK_VERSION=3.5.1
ENV SPARK_PACKAGE=spark-${SPARK_VERSION}-bin-hadoop3

RUN curl -k -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_PACKAGE}.tgz | tar -xz -C /opt

ENV SPARK_HOME=/opt/${SPARK_PACKAGE}
ENV PATH=$SPARK_HOME/bin:$PATH

COPY requirements.txt requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt

WORKDIR /app
EXPOSE 8888
