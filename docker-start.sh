#!/bin/bash

# start 1-node zookeeper
docker run -d -ti --publish 2181:2181 --name zookeeper vasylpurchel/zookeeper

# start 3-node kafka server
ZOOKEEPER_HOST=192.168.99.100

docker run -d -ti -e KAFKA_HEAP_OPTS="-Xmx64M -Xms64M" -e KAFKA_BROKER_ID=1 -e KAFKA_ZOOKEEPER_CONNECT="$ZOOKEEPER_HOST:2181" -e KAFKA_ADVERTISED_PORT=9092 -e KAFKA_ADVERTISED_HOST_NAME="192.168.99.100" --publish 9092:9092 --name kafka-node-1 vasylpurchel/kafka

docker run -d -ti -e KAFKA_HEAP_OPTS="-Xmx64M -Xms64M" -e KAFKA_BROKER_ID=2 -e KAFKA_ZOOKEEPER_CONNECT="$ZOOKEEPER_HOST:2181" -e KAFKA_ADVERTISED_PORT=9093 -e KAFKA_ADVERTISED_HOST_NAME="192.168.99.100" --publish 9093:9092 --name kafka-node-2 vasylpurchel/kafka

docker run -d -ti -e KAFKA_HEAP_OPTS="-Xmx64M -Xms64M" -e KAFKA_BROKER_ID=3 -e KAFKA_ZOOKEEPER_CONNECT="$ZOOKEEPER_HOST:2181" -e KAFKA_ADVERTISED_PORT=9094 -e KAFKA_ADVERTISED_HOST_NAME="192.168.99.100" --publish 9094:9092 --name kafka-node-3 vasylpurchel/kafka

sleep 15s

# test kafka server
docker run -d -ti --name kafka-test vasylpurchel/kafka kafka-topics.sh --create --zookeeper $ZOOKEEPER_HOST:2181 --replication-factor 2 --partitions 3 --topic test

sleep 10s

echo $(docker logs kafka-test)
