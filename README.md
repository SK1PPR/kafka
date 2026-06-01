# Kafka C++ Clone

A small Kafka-compatible broker written in C++.

This project implements the core pieces of a Kafka broker from the wire protocol upward: TCP framing, request decoding, response encoding, persistent client connections, and concurrent client handling.

## Features

- TCP broker listening on port `9092`
- Kafka request frame parsing
- Big-endian protocol encoder and decoder
- `ApiVersions` request handling
- Persistent client connections
- Concurrent clients using one thread per connection
- Simple server, connection, and request handler abstractions

## Project Structure

```text
include/kafka/
  connection.hpp
  request.hpp
  request_handler.hpp
  server.hpp
  protocol/
    decoder.hpp
    encoder.hpp

src/
  connection.cpp
  main.cpp
  request.cpp
  request_handler.cpp
  server.cpp
  protocol/
    decoder.cpp
    encoder.cpp
```

## Build

```sh
make
```

The executable is written to:

```text
build/kafka
```

## Run

```sh
./build/kafka
```

## Clean

```sh
make clean
```

## Notes

This is an educational Kafka clone, not a production broker. The current focus is protocol correctness, connection handling, and building the broker architecture incrementally.
