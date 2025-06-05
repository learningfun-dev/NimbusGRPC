# NimbusGRPC

NimbusGRPC is a high-throughput, scalable event processing system built using **gRPC**, **Kafka**, and **Redis**. Designed for mobile and IoT workloads, it provides a hybrid real-time communication layer using bi-directional streaming and a robust pub/sub architecture.

## Features

- ✅ Bi-directional gRPC stream handling for real-time client-server communication.
- 🚀 Redis-based fan-out with pub/sub for distributing processed results back to the correct server instance.
- 📡 Kafka integration for scalable, durable, and ordered event ingestion and processing.
- ⚙️ Centralized configuration management.
- 🧩 Modularized Kafka producer, Kafka consumers, and Redis client components.
- 📲 Client ID-based stream routing for sending real-time responses to specific clients.
- 🔐 Thread-safe stream management on the gRPC server.
- 🔄 Two-stage Kafka processing pipeline:
    1.  Event ingestion and initial processing (e.g., "sq" operation).
    1.  Result forwarding to Redis for client notification.

## Architecture Overview

This section details the event flow specifically for the NimbusGRPC system, which leverages gRPC for client communication, Kafka for event processing, and Redis for real-time result delivery. A key aspect of this architecture is the use of Protocol Buffers (Protobuf) for efficient message serialization throughout the Kafka pipeline and pod-specific Redis channels for scalable fan-out.

Here's a diagram illustrating the flow:

```mermaid
graph TD
    subgraph "NimbusGRPC Client & Server"
        Client["Mobile/IoT Client (gRPC)"] -- "1\. EventRequest (Proto over gRPC Stream)" --> gRPCServer["NimbusGRPC Server Pod"]
        gRPCServer -- "9\. EventResponse (Proto over gRPC Stream)" --> Client
    end

    subgraph "Kafka Event Processing Pipeline (Protobuf Messages)"
        gRPCServer -- "2\. KafkaEventRequest (Proto, includes pod-specific RedisChannel)" --> KafkaEvents["Kafka (Events Topic - Protobuf)"]
        KafkaEvents -- "3\. Consume Proto" --> EventProcessor["Kafka Consumer (Event Processor)"]
        EventProcessor -- "4\. Process event (e.g., sq(n))" --> EventProcessor
        EventProcessor -- "5\. KafkaEventResponse (Proto, includes pod-specific RedisChannel)" --> KafkaResults["Kafka (Results Topic - Protobuf)"]
        KafkaResults -- "6\. Consume Proto" --> ResultForwarder["Kafka Consumer (Result Forwarder)"]
    end

    subgraph "Redis Pub/Sub for Real-time Delivery (Pod-Specific Channels)"
        ResultForwarder -- "7\. SPUBLISH KafkaEventResponse (Proto) to Pod-Specific RedisChannel" --> Redis["Redis Cluster Pub/Sub"]
        Redis -- "8\. SSUBSCRIBE & Receive on own Pod-Specific Channel" --> gRPCServer
    end

    subgraph "Optional Database Persistence"
        EventProcessor -- "10a. Store/Update Processed Data" --> Database["(Database)"]
        KafkaEvents -- "10b. Consume for Auditing" --> EventDBWriter["Kafka Consumer (Event Archiver)"]
        EventDBWriter -- "10c. Persist Raw Event" --> Database
    end
```

This diagram shows:

### Core Real-time Flow:

This diagram shows the following flow for NimbusGRPC:

1. The **Mobile/IoT Client** sends an `EventRequest` (serialized as Protobuf) via a gRPC bidirectional stream to a specific **NimbusGRPC Server Pod**.

1. The **NimbusGRPC Server Pod** receives the Protobuf message. It then embeds its own unique, pod-specific Redis channel name (e.g., `results:grpc-pod-456`) into the `RedisChannel` field of a `KafkaEventRequest` (Protobuf). This `KafkaEventRequest` is then published to the **Kafka (Events Topic)**. All messages in this Kafka topic are Protobuf encoded.

1. A **Kafka Consumer (Event Processor)**, part of a consumer group, subscribes to the "Events Topic." It deserializes the Protobuf `KafkaEventRequest`.

1. The **Event Processor** performs the necessary business logic (e.g., squaring a number). The pod-specific `RedisChannel` is carried through.

1. The **Event Processor** creates a `KafkaEventResponse` (also Protobuf), ensuring it includes the original `client_id` and the critical pod-specific `RedisChannel`. This response is published to the **Kafka (Results Topic)**, again using Protobuf.

1. A **Kafka Consumer (Result Forwarder)**, part of another consumer group, subscribes to the "Results Topic" and deserializes the Protobuf `KafkaEventResponse`.

1. The **Result Forwarder** reads the pod-specific `RedisChannel` from the message. It then uses Redis Cluster's `SPUBLISH` command to send the `KafkaEventResponse` (still as a Protobuf byte array) only to that designated pod-specific channel in **Redis Cluster Pub/Sub**.

1. The target **NimbusGRPC Server Pod**, which had previously subscribed to its unique pod-specific channel using `SSUBSCRIBE`, receives the message from Redis.

1. The **NimbusGRPC Server Pod** deserializes the message, identifies the correct client gRPC stream using the `client_id` from the message, and sends the `EventResponse` (Protobuf) back to the Client over the gRPC stream.

(Optional Database Interaction)

`10a` The **Event Processor** can also interact with a Database to store or update data based on the processed event.

`10b, 10c` A separate **Kafka Consumer (Event Archiver)** could consume from the `Kafka (Events Topic)` to persist raw Protobuf events into a Database for auditing, analytics, or long-term storage.

This architecture leverages Protobuf for efficient data handling throughout the Kafka pipeline and uses pod-specific Redis channels (optimally with `SPUBLISH`/`SSUBSCRIBE` in a Redis Cluster) to ensure scalable and targeted real-time delivery of results back to the correct gRPC server pod and subsequently to the client.

## Key Files & Packages

-   **`nimbus/client/`**: Sample gRPC client implementation demonstrating how to connect, send metadata, and handle bidirectional streams with the NimbusService.
-   **`nimbus/config/`**: Centralized configuration management for the application, loading settings from environment variables or defaults.
-   **`nimbus/kafkaconsumer/`**: Contains the logic for two distinct Kafka consumers:
    -   `event_consumer.go`: Consumes raw events from `KafkaEventsTopic`, processes them (e.g., performs calculations like squaring a number), and publishes results to `KafkaResultsTopic`.
    -   `result_consumer.go`: Consumes processed results from `KafkaResultsTopic` and publishes them to the appropriate Redis channel for fan-out to the gRPC server instances.
-   **`nimbus/kafkaproducer/`**: Manages the Kafka producer instance, providing functions to publish messages (both event requests and event responses) to specified Kafka topics.
-   **`nimbus/proto/`**: Contains the Protobuf definitions (`.proto` files) for the gRPC service (`NimbusService`) and the message types (`EventRequest`, `EventResponse`, `KafkaEventRequest`, `KafkaEventResponse`).
-   **`nimbus/redisclient/`**: Handles the Redis client lifecycle (initialization, access, closing) and provides functionalities for publishing messages (`publisher.go`) and subscribing to channels (`subscriber.go` - used by the gRPC server).
-   **`nimbus/server/`**: Core gRPC server implementation (`main.go`, `events.go`). It handles incoming client connections, manages client streams, forwards events to Kafka (via `kafkaproducer`), and subscribes to Redis (via `redisclient`) to receive processed results and send them back to the appropriate clients.
-   **`nimbus/cmd/kafkaconsumers/main.go`**: The main application executable for running both the `EventConsumer` and `ResultConsumer`.
-   **`nimbus/cmd/server/main.go`**: The main application executable for running the gRPC server.
-   **`nimbus/cmd/client/main.go`**: The main application executable for running the sample gRPC client.


## How to Run

1.  **Clone the repo:**
    ```bash
    git clone [https://github.com/learningfun-dev/NimbusGRPC.git](https://github.com/learningfun-dev/NimbusGRPC.git)
    cd NimbusGRPC
    ```

1.  **Install dependencies:**
    Ensure Go is installed. Then, fetch the Go module dependencies.
    ```bash
    go mod tidy
    ```

1.  **Set up external services (Redis & Kafka):**
    Make sure Redis and Kafka are running and accessible. You can use Docker Compose for a quick setup:
    ```bash
    docker compose --profile dev up -d
    ```
    Ensure the connection details in your configuration (or environment variables) match your Redis and Kafka setup.

1.  **Configure Environment Variables (Optional but Recommended):**
   The application uses environment variables for configuration (see `nimbus/config/config.go`). You can set these in your shell, or for easier management during development, update the `.env` file in the root of your project.

    **Example `.env` file:**
    ```env
    NIMBUS_PORT=50051
    NIMBUS_REDIS_ADDRESS="localhost:6379"
    NIMBUS_KAFKA_BROKERS="localhost:9092"
    NIMBUS_REDIS_EVENTS_CHANNEL="events_results_nimbus_pod"
    NIMBUS_KAFKA_EVENTS_TOPIC="nimbus-events-v1"
    NIMBUS_KAFKA_RESULTS_TOPIC="nimbus-results-v1"
    NIMBUS_SHUTDOWN_TIMEOUT_SEC=20
    ```

    

1.  **Build the applications:**
    The provided `Makefile` (or your build process) should build the gRPC server, the Kafka consumers service, and the sample client.
    ```bash
    make nimbus
    ```
    This command should create executables, for example, in a `bin/` directory:
    * `./bin/nimbus/cmd/server` (The gRPC server)
    * `./bin/nimbus/cmd/kafkaconsumer` (The combined Kafka consumer service)
    * `./bin/nimbus/cmd/client` (The sample gRPC client)

1.  **Start the gRPC server:**
    ```bash
    ./bin/nimbus/cmd/server
    ```
    This starts the NimbusService, listening for incoming gRPC connections. It will publish received events to the `NIMBUS_KAFKA_EVENTS_TOPIC` and subscribe to Redis on `NIMBUS_REDIS_EVENTS_CHANNEL` for results.

1.  **Start the Kafka consumer service:**
    ```bash
    ./bin/nimbus/cmd/kafkaconsumer
    ```
    This service runs two consumers:
    * **Event Consumer**: Reads from `NIMBUS_KAFKA_EVENTS_TOPIC`, processes events (e.g., for an "sq" event, it calculates the square of the number), and publishes a `KafkaEventResponse` to `NIMBUS_KAFKA_RESULTS_TOPIC`.
    * **Result Consumer**: Reads the `KafkaEventResponse` from `NIMBUS_KAFKA_RESULTS_TOPIC` and publishes it to the Redis channel specified in the message's `RedisChannel` field (which should match what the gRPC server is listening on).

1.  **Start the gRPC client (example):**
    ```bash
    ./bin/nimbus/cmd/client --client_id=client_test_001 --start=1 --end=10
    ```
    This will start the sample client, which connects to the gRPC server. It sends a `client_id` in the metadata and then streams a series of "sq" event requests for numbers from `--start` to `--end`. The client will then wait to receive processed results back from the server on the same stream.

    You can run multiple client instances with different `client_id` values.

## Requirements

-   Go (version 1.20+ recommended)
-   Redis (running instance)
-   Kafka (running instance with topics created or auto-creation enabled)
-   Protocol Buffer Compiler (`protoc`) for regenerating Go code from `.proto` files if you modify them.
