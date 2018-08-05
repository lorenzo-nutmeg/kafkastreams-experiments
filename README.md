# Experiments with Kafka Streams

Each module is a separate experiment

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