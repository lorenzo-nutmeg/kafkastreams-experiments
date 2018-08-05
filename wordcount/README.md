# Word count

This is the basic word count example.
This implementation is a mix of "Kafka: The definitive guide" book, Spring-Kafka documentation and Kafka Streams documentation.

It uses Spring Kafka but not the Spring Kafka support for generating StreamBuilder.

The application reads the content of a book (War and Peace, to pick a short one) and counts occurrences of each word excluding
some words (e.g. articles).

The application is not meant to be parallelised.