# batch_streaming

## Table of Contents
- [Introduction](#introduction)
- [Project Architecture](#project-architecture)
- [Container Creation](#container-creation)
- [Web Services](#web-services)

## Introduction

This project serves as an illustration to how to build an end-to-end batch and real-time data pipelines. It covers real-time aspects of data ingestion, processing and lastly storage using various tech stacks that include Python, Apache Kafka, Apache Zookeeper, DuckDb and Postgres. Last but not least, Docker is used to containerize each of the services essential for this project.

## Project Architecture

![Project Architecture]()

Essential components for this project are:
- **Data Source**: `measurements.parquet` as the starting point of the project to generate random user data.
- **Apache Kafka and Zookeeper**: Used for streaming data from `measurements.parquet` to the processing engine.
    - **Kafka UI**: Review and controll the Kafka clusters&Schema registries.
- **DuckDB**: Source database and data processor unit of the system. 
- **Postgres**: Where the data will be stored(Semantic).

## Container Creation

We can get the container up and running with the necessary services by issuing the command below, in your terminal :

```bash
docker compose up
```

If you encounter a docker network error or want to create a separate network for docker services, create one via command:
```bash
docker network create <new-network-name>
```
Make sure you update `networks` in the docker-compose-infra file with the new network name.

You now should have the containers running smoothly. You may now run the `kafka_producer.py` with:
```bash
python kafka_producer.py
```
`kafka_producer.py` will start reading the measurements.parquet file and start queueing entries into Kafka topic.

While producer is running, open an other terminal on your IDE, and run the `kafka_consumer.py` with:
```bash
python kafka_consumer.py
```

This will start reading entries(both existing&to be produced entries) from kafka topic and write them to DuckDB database.


## Web Services

When the container is up and running, you may now access Kafka UI to take a look at your kafka topic&producer&consumer. You now can access the Kafka UI at `https://localhost:8888`
