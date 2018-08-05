# Experiments with Kafka Streams

Each module is a separate experiment

Working experiments, so far:
- [Word Count](./wordcount): the mandatory "hello, world" example for stream processing
- [Stock Stats](./stockstats): stateful, windowed aggregation; AVRO with objects generated from IDL

All experiments uses a [local Kafka cluster](./docker), 3 Kafka brokers, ZooKeeper and Schema Registry, 
running with Docker Compose. The `docker-compose.yml` file works with Docker for Mac.

## Running experiments

To start the cluster (from `./docker` dir):
```
$ docker-compose up -d && ./wait_brokers_up.sh
```

To run the experiment (from each experiment module directory):
```
$ mvn spring-boot:run
```

To tear down the cluster:
```
$ docker-compose down 
```


## Useful kafka commands

Consume from a topic showing both key and value:
```
kafka-console-consumer --bootstrap-server localhost:29092 \
    --topic <topic-name> --from-beginning \
    --property print.key=true --property key.separator=":"
```

Consume from a topic using Avro, showing both key and value:
```
kafka-avro-console-consumer --bootstrap-server localhost:19092 \
    --topic stockstats.stats --from-beginning \
    --property print.key=true --property schema.registry.url=http://localhost:18081 \
    --property key.separator=":" 
```