#!/usr/bin/env bash

kafka-topics --zookeeper localhost:2181 --create --topic employee_details --partition 1 --replication-factor 1 --config cleanup.policy=compact --config min.cleanable.dirty.ratio=0.002 --config segment.ms=4000

kafka-topics --zookeeper localhost:2181 --describe --topic employee_details

kafka-console-consumer --bootstrap-server localhost:9092 --topic employee_details --from-beginning --property print.key=true --property key.separator=,

kafka-console-producer --broker-list 127.0.0.1:9092 --topic employee_details --property print.key=true --property key.separator=,