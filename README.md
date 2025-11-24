# Kafka Producer/Consumer

Production-ready Kafka messaging system with advanced features for reliable message processing.

## Features

**Producer:**
- Idempotent delivery (prevents duplicates)
- User-based partitioning for consistent routing
- Exponential backoff retry logic
- Real-time metrics tracking
- Graceful shutdown with signal handling
- Pydantic message validation
- Structured logging

**Consumer:**
- Dead Letter Queue for failed messages
- Circuit breaker pattern
- Batch processing
- Manual offset management
- Retry mechanism with exponential backoff
- Health check endpoints
- Graceful shutdown

**Infrastructure:**
- Environment-based configuration with validation
- Type-safe message schemas
- Built-in metrics collection
- Comprehensive error handling

## Prerequisites

- Python 3.8+
- Docker & Docker Compose
- Kafka (included in docker-compose.yaml)

## Installation

Clone the repository:
```bash
git clone <repository-url>
cd Kafka-backend
```

Install dependencies:
```bash
pip install -r requirements.txt
```

Start Kafka:
```bash
docker-compose up -d
```

Configure environment:
```bash
cp .env.example .env
# Edit .env with your settings
```

## Quick Start

Run the producer:
```bash
python producer.py
```

Run the consumer (in another terminal):
```bash
python consumer.py
```

The producer sends sample orders to the `orders` topic. The consumer processes them and routes failures to the DLQ.

## Architecture

```
┌─────────────────┐
│   Producer      │
│  - Validation   │
│  - Partitioning │
│  - Retry Logic  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│     Kafka       │
│  orders topic   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐       ┌─────────────────┐
│   Consumer      │──────▶│   Dead Letter   │
│                 │ fail  │   Queue (DLQ)   │
└─────────────────┘       └─────────────────┘
```

## Configuration

## Message Schema

```python
{
    "order_id": "uuid4",                    # Unique identifier
    "user": "string",                       # Username (partition key)
    "item": "string",                       # Item name
    "quantity": int,                        # Quantity (1-10000)
    "price": float,                         # Price per item (optional)
    "timestamp": "ISO8601",                 # Order timestamp
    "priority": "low|normal|high|urgent"    # Processing priority
}
```

## Metrics

The system collects metrics for both producer and consumer.

**Producer:**
- `producer.messages_sent`: Total messages sent
- `producer.delivery_success`: Successful deliveries
- `producer.delivery_failed`: Failed deliveries
- `producer.send_latency`: Average send latency
- `producer.send_errors`: Send errors

**Consumer:**
- `consumer.messages_processed`: Successfully processed messages
- `consumer.processing_latency`: Processing time
- `consumer.processing_errors`: Processing failures
- `consumer.dlq_sent`: Messages sent to DLQ
- `consumer.circuit_breaker_open`: Circuit breaker activations
- `consumer.commits`: Offset commits

Set `ENABLE_METRICS=true` to enable metrics collection. A summary is printed on shutdown.

## Monitoring

**Health Check:**

```python
from consumer import EnhancedConsumer
from config import KafkaConfig

consumer = EnhancedConsumer(KafkaConfig.from_env())
health = consumer.health_check()
print(health)
# {'status': 'healthy', 'running': True, 'circuit_breaker_state': 'closed', ...}
```

**Logs:**

```
2025-11-21 10:30:45 - producer - INFO - [producer.py:123] - Message delivered to orders[2] at offset 42
2025-11-21 10:30:46 - consumer - INFO - [consumer.py:234] - Processing order abc-123: 5x laptop from alice
```

## Error Handling

**Retry Strategy:**

1. **Transient Errors**: Exponential backoff (1s, 2s, 4s, ...)
2. **Validation Errors**: Immediate DLQ routing
3. **Max Retries**: After 3 attempts, message sent to DLQ
4. **Circuit Breaker**: Opens after 5 consecutive failures, resets after 60s

**Dead Letter Queue:**

Failed messages are enriched with metadata:
```json
{
    "original_topic": "orders",
    "original_partition": 1,
    "original_offset": 123,
    "original_value": "...",
    "error": "Validation error: quantity exceeds maximum",
    "timestamp": 1700567890.123
}
```

## Testing

Start Kafka:
```bash
docker-compose up -d
```

Run producer:
```bash
python producer.py
```

Run consumer (different terminal):
```bash
python consumer.py
```

Check DLQ for failed messages:
```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic orders-dlq \
  --from-beginning
```

To simulate failures, modify the processor in `consumer.py`:

```python
def custom_processor(message: OrderMessage) -> bool:
    if message.item == "test-failure":
        raise Exception("Simulated failure")
    return True
```

## Scalability and security

**Security:**
- Enable SASL/SSL for production Kafka clusters
- Use secrets management for credentials
- Implement authentication/authorization

**Scaling:**
- Increase partition count for parallel processing
- Deploy multiple consumer instances (same group ID)
- Adjust `max.poll.records` based on message size

**Monitoring:**
- Integrate with Prometheus/Grafana
- Set up alerts for DLQ accumulation
- Monitor consumer lag

**Performance:**
- Adjust `linger.ms` and `batch.size` for throughput
- Tune `session.timeout.ms` for failure detection
- Configure compression (`snappy`, `lz4`, `gzip`)

## Troubleshooting

**Consumer not processing messages:**
1. Check consumer group ID isn't in use
2. Verify topic exists and has messages
3. Check offset reset strategy

**Messages going to DLQ:**
1. Review DLQ messages for error patterns
2. Check validation rules in schemas
3. Verify external dependencies are available

**High latency:**
1. Reduce `linger.ms` for lower latency
2. Increase partitions for parallelism
3. Check network latency to Kafka brokers

