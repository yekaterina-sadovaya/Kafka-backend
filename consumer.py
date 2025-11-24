import json
import signal
import time
from typing import List, Optional, Callable
from confluent_kafka import Consumer, KafkaException, KafkaError, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from config import KafkaConfig
from schemas import OrderMessage
from utils import MetricsCollector, Timer, setup_logger, exponential_backoff


class CircuitBreaker:
    # Circuit breaker to prevent cascade failures
    def __init__(self, failure_threshold: int = 5, timeout_seconds: float = 60.0):
        self.failure_threshold = failure_threshold
        self.timeout_seconds = timeout_seconds
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "closed"  # closed, open, half-open
    
    def record_success(self):
        self.failure_count = 0
        self.state = "closed"
    
    def record_failure(self):
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.failure_threshold:
            self.state = "open"
    
    def can_proceed(self) -> bool:
        # Check if operation should proceed
        if self.state == "closed":
            return True
        
        if self.state == "open":
            # Check if timeout has elapsed
            if time.time() - self.last_failure_time >= self.timeout_seconds:
                self.state = "half-open"
                self.failure_count = 0
                return True
            return False
        
        # half-open state: allow one attempt
        return True


class EnhancedConsumer:
    def __init__(self, config: KafkaConfig, message_processor: Optional[Callable] = None):
        self.config = config
        self.logger = setup_logger(__name__, config.log_level)
        self.metrics = MetricsCollector() if config.enable_metrics else None
        self.consumer = Consumer(config.get_consumer_config())
        self.message_processor = message_processor or self._default_processor
        self.circuit_breaker = CircuitBreaker()
        self.shutdown_requested = False
        self.running = False
        
        # DLQ producer (for failed messages)
        from confluent_kafka import Producer
        self.dlq_producer = Producer({
            'bootstrap.servers': config.bootstrap_servers,
            'enable.idempotence': True
        })
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        self.logger.info("Enhanced Consumer initialized")
        self._ensure_dlq_topic_exists()
    
    def _signal_handler(self, signum, frame):
        self.logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.shutdown_requested = True
    
    def _ensure_dlq_topic_exists(self):
        admin_client = AdminClient({'bootstrap.servers': self.config.bootstrap_servers})
        
        dlq_topic = NewTopic(
            self.config.dlq_topic_name,
            num_partitions=1,
            replication_factor=1
        )
        
        try:
            fs = admin_client.create_topics([dlq_topic], request_timeout=10.0)
            for topic, f in fs.items():
                try:
                    f.result()
                    self.logger.info(f"DLQ topic {topic} created")
                except KafkaException as e:
                    if e.args[0].code() == 36:  # TOPIC_ALREADY_EXISTS
                        self.logger.debug(f"DLQ topic {topic} already exists")
                    else:
                        self.logger.warning(f"Failed to create DLQ topic: {e}")
        except Exception as e:
            self.logger.warning(f"Error ensuring DLQ topic exists: {e}")
    
    def _send_to_dlq(self, original_msg, error: str):
        try:
            dlq_value = {
                'original_topic': original_msg.topic(),
                'original_partition': original_msg.partition(),
                'original_offset': original_msg.offset(),
                'original_key': original_msg.key().decode('utf-8') if original_msg.key() else None,
                'original_value': original_msg.value().decode('utf-8'),
                'error': error,
                'timestamp': time.time()
            }
            
            self.dlq_producer.produce(
                topic=self.config.dlq_topic_name,
                value=json.dumps(dlq_value).encode('utf-8'),
                key=original_msg.key()
            )
            self.dlq_producer.poll(0)
            
            if self.metrics:
                self.metrics.increment('consumer.dlq_sent')
            
            self.logger.warning(f"Message sent to DLQ: {error}")
            
        except Exception as e:
            self.logger.error(f"Failed to send message to DLQ: {e}")
            if self.metrics:
                self.metrics.increment('consumer.dlq_failed')
    
    def _default_processor(self, message: OrderMessage) -> bool:
        """
        Args:
            message: Parsed OrderMessage
        
        Returns:
            True if processing successful, False otherwise
        """
        self.logger.info(
            f"Processing order {message.order_id}: "
            f"{message.quantity}x {message.item} "
            f"from {message.user} "
            f"(priority: {message.priority})"
        )
        
        # Simulate processing time
        time.sleep(0.1)
        
        # Simulate occasional failures for demonstration
        # (in production, this would be your actual business logic)
        if message.quantity > 100:
            raise ValueError("Quantity exceeds maximum allowed")
        
        return True
    
    def _process_message(self, msg) -> bool:
        """
        Args:
            msg: Kafka message
        
        Returns:
            True if processing successful, False otherwise
        """
        retry_count = 0
        max_retries = self.config.max_retries
        
        while retry_count <= max_retries:
            try:
                with Timer(self.metrics, 'consumer.processing_latency'):
                    # Parse message
                    value = msg.value().decode('utf-8')
                    order = OrderMessage.from_json(value)
                    
                    # Check circuit breaker
                    if not self.circuit_breaker.can_proceed():
                        self.logger.warning("Circuit breaker open, skipping processing")
                        if self.metrics:
                            self.metrics.increment('consumer.circuit_breaker_open')
                        return False
                    
                    # Process message
                    success = self.message_processor(order)
                    
                    if success:
                        self.circuit_breaker.record_success()
                        if self.metrics:
                            self.metrics.increment('consumer.messages_processed')
                        return True
                    else:
                        raise Exception("Processor returned False")
                
            except json.JSONDecodeError as e:
                self.logger.error(f"Invalid JSON: {e}")
                self._send_to_dlq(msg, f"JSON decode error: {str(e)}")
                if self.metrics:
                    self.metrics.increment('consumer.invalid_json')
                return False  # Don't retry JSON errors
            
            except ValueError as e:
                self.logger.error(f"Validation error: {e}")
                self._send_to_dlq(msg, f"Validation error: {str(e)}")
                if self.metrics:
                    self.metrics.increment('consumer.validation_errors')
                return False  # Don't retry validation errors
            
            except Exception as e:
                self.logger.error(f"Processing error on attempt {retry_count + 1}: {e}")
                self.circuit_breaker.record_failure()
                
                if self.metrics:
                    self.metrics.increment('consumer.processing_errors')
                
                if retry_count < max_retries:
                    backoff = exponential_backoff(retry_count, self.config.retry_backoff_ms)
                    self.logger.info(f"Retrying in {backoff:.2f}s...")
                    time.sleep(backoff)
                    retry_count += 1
                else:
                    self.logger.error(f"Max retries ({max_retries}) exceeded")
                    self._send_to_dlq(msg, f"Max retries exceeded: {str(e)}")
                    if self.metrics:
                        self.metrics.increment('consumer.max_retries_exceeded')
                    return False
        
        return False
    
    def _process_batch(self, messages: List) -> int:
        """
        Args:
            messages: List of Kafka messages
        
        Returns:
            Number of successfully processed messages
        """
        success_count = 0
        
        for msg in messages:
            # Process a batch of messages
            if self.shutdown_requested:
                break
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    self.logger.debug(f"Reached end of partition {msg.partition()}")
                else:
                    self.logger.error(f"Consumer error: {msg.error()}")
                    if self.metrics:
                        self.metrics.increment('consumer.kafka_errors')
                continue
            
            if self._process_message(msg):
                success_count += 1
        
        return success_count
    
    def start(self, topics: Optional[List[str]] = None):
        """
        Start consuming messages.
        
        Args:
            topics: List of topics to subscribe to (defaults to config topic)
        """
        if topics is None:
            topics = [self.config.topic_name]
        
        self.consumer.subscribe(topics)
        self.running = True
        
        self.logger.info(f"Consumer started, subscribed to: {topics}")
        
        message_batch = []
        last_commit_time = time.time()
        commit_interval = 5.0
        
        try:
            while not self.shutdown_requested:
                # Poll for messages
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    # No message, process any pending batch
                    if message_batch:
                        self._process_batch(message_batch)
                        message_batch.clear()
                    continue
                
                message_batch.append(msg)
                
                # Process batch when it reaches max size
                if len(message_batch) >= self.config.consumer_max_poll_records:
                    self.logger.debug(f"Processing batch of {len(message_batch)} messages")
                    self._process_batch(message_batch)
                    message_batch.clear()
                
                # Commit offsets periodically
                current_time = time.time()
                if current_time - last_commit_time >= commit_interval:
                    try:
                        self.consumer.commit(asynchronous=False)
                        self.logger.debug("Offsets committed")
                        if self.metrics:
                            self.metrics.increment('consumer.commits')
                        last_commit_time = current_time
                    except KafkaException as e:
                        self.logger.error(f"Failed to commit offsets: {e}")
                        if self.metrics:
                            self.metrics.increment('consumer.commit_errors')
        
        except Exception as e:
            self.logger.error(f"Unexpected error in consumer loop: {e}")
            if self.metrics:
                self.metrics.increment('consumer.unexpected_errors')
        
        finally:
            self._shutdown()
    
    def _shutdown(self):
        self.logger.info("Shutting down consumer...")
        
        try:
            # Final commit
            self.consumer.commit()
            self.logger.info("Final offset commit completed")
        except Exception as e:
            self.logger.error(f"Error during final commit: {e}")
        
        # Close consumer
        self.consumer.close()
        
        # Flush DLQ producer
        remaining = self.dlq_producer.flush(timeout=10.0)
        if remaining > 0:
            self.logger.warning(f"{remaining} DLQ messages pending")
        
        # Print metrics
        if self.metrics:
            self.metrics.print_summary()
        
        self.running = False
        self.logger.info("Consumer shutdown complete")
    
    def health_check(self) -> dict:
        return {
            'status': 'healthy' if self.running and not self.shutdown_requested else 'unhealthy',
            'running': self.running,
            'circuit_breaker_state': self.circuit_breaker.state,
            'metrics': self.metrics.get_metrics() if self.metrics else None
        }


def main():
    config = KafkaConfig.from_env()
    
    # Message processor
    def custom_processor(message: OrderMessage) -> bool:
        print(f"📦 Order received: {message.order_id}")
        print(f"   User: {message.user}")
        print(f"   Item: {message.item}")
        print(f"   Quantity: {message.quantity}")
        print(f"   Priority: {message.priority}")
        if message.price:
            print(f"   Total: ${message.price * message.quantity:.2f}")
        print()
        
        # Your business logic here
        # Return True for success, False or raise exception for failure
        return True
    
    # Create and start consumer
    consumer = EnhancedConsumer(config, message_processor=custom_processor)
    consumer.start()


if __name__ == "__main__":
    main()
