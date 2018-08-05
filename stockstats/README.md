# Stock stats

Based on the example from "Kafka: The definitive guide book" ([source code](https://github.com/gwenshap/kafka-streams-stockstats) from the book).
The implementation has been changed not to use the new API for KStreams 1.0.1 (a lot has changed) and to use AVRO.

AVRO model objects are generated from AVRO IDL.

A producer generates random trades and send them to the source topic.

The Kafka Stream application calculates statistics on a 5 sec window using a persistent state-store (RockDB).

A Consumer reads the stats and print them. 