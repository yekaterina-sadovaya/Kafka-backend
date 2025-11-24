import json
import signal
import sys
import time
from typing import Optional, Callable
from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from config import KafkaConfig
from schemas import OrderMessage, MessageMetadata
from utils import MetricsCollector, Timer, setup_logger, exponential_backoff


class EnhancedProducer:
    def __init__(self, config: KafkaConfig):
        self.config = config
        self.logger = setup_logger(__name__, config.log_level)
        self.metrics = MetricsCollector() if config.enable_metrics else None
        self.producer = Producer(config.get_producer_config())
        self.shutdown_requested = False
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        self.logger.info("Enhanced Producer initialized")
        self._ensure_topics_exist()
    
    def _signal_handler(self, signum, frame):
        self.logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.shutdown_requested = True
    
    def _ensure_topics_exist(self):
        admin_client = AdminClient({'bootstrap.servers': self.config.bootstrap_servers})
        
        topics = [
            NewTopic(self.config.topic_name, num_partitions=3, replication_factor=1),
            NewTopic(self.config.dlq_topic_name, num_partitions=1, replication_factor=1)
        ]
        
        try:
            # Create topics (idempotent operation)
            fs = admin_client.create_topics(topics, request_timeout=10.0)
            for topic, f in fs.items():
                try:
                    f.result()
                    self.logger.info(f"Topic {topic} created successfully")
                except KafkaException as e:
                    if e.args[0].code() == 36:  # TOPIC_ALREADY_EXISTS
                        self.logger.debug(f"Topic {topic} already exists")
                    else:
                        self.logger.warning(f"Failed to create topic {topic}: {e}")
        except Exception as e:
            self.logger.warning(f"Error ensuring topics exist: {e}")
    
    def _delivery_callback(self, err, msg):
        if err:
            self.logger.error(f"Message delivery failed: {err}")
            if self.metrics:
                self.metrics.increment('producer.delivery_failed')
        else:
            self.logger.info(
                f"Message delivered to {msg.topic()}[{msg.partition()}] "
                f"at offset {msg.offset()}"
            )
            if self.metrics:
                self.metrics.increment('producer.delivery_success')
    
    def send_message(self, message: OrderMessage, callback: Optional[Callable] = None, headers: Optional[dict] = None) -> bool:
        """
        Args:
            message: OrderMessage to send
            callback: Optional custom delivery callback
            headers: Optional message headers
        
        Returns:
            True if message queued successfully, False otherwise
        """
        if self.shutdown_requested:
            self.logger.warning("Shutdown requested, not sending message")
            return False
        
        # Use custom callback or default
        delivery_callback = callback or self._delivery_callback
        
        retry_count = 0
        max_retries = self.config.max_retries
        
        while retry_count <= max_retries:
            try:
                with Timer(self.metrics, 'producer.send_latency'):
                    partition_key = message.get_partition_key()
                    
                    # Prepare headers
                    kafka_headers = []
                    if headers:
                        kafka_headers = [(k, str(v).encode('utf-8')) for k, v in headers.items()]
                    
                    # Add metadata headers
                    kafka_headers.append(('retry_count', str(retry_count).encode('utf-8')))
                    kafka_headers.append(('source', 'enhanced-producer'.encode('utf-8')))
                    
                    # Produce message
                    self.producer.produce(
                        topic=self.config.topic_name,
                        key=partition_key.encode('utf-8'),
                        value=message.to_json().encode('utf-8'),
                        callback=delivery_callback,
                        headers=kafka_headers
                    )
                    
                    # Trigger delivery reports
                    self.producer.poll(0)
                    
                    if self.metrics:
                        self.metrics.increment('producer.messages_sent')
                    
                    self.logger.debug(f"Message queued: {message.order_id}")
                    return True
                    
            except BufferError:
                # if ocal queue is full, wait and retry
                self.logger.warning("Local queue full, waiting...")
                self.producer.poll(1)
                
            except KafkaException as e:
                self.logger.error(f"Kafka error on attempt {retry_count + 1}: {e}")
                if self.metrics:
                    self.metrics.increment('producer.send_errors')
                
                if retry_count < max_retries:
                    backoff = exponential_backoff(retry_count, self.config.retry_backoff_ms)
                    self.logger.info(f"Retrying in {backoff:.2f}s...")
                    time.sleep(backoff)
                    retry_count += 1
                else:
                    self.logger.error(f"Max retries ({max_retries}) exceeded")
                    if self.metrics:
                        self.metrics.increment('producer.max_retries_exceeded')
                    return False
            
            except Exception as e:
                self.logger.error(f"Unexpected error: {e}")
                if self.metrics:
                    self.metrics.increment('producer.unexpected_errors')
                return False
        
        return False
    
    def flush(self, timeout: float = 10.0):
        self.logger.info("Flushing pending messages...")
        remaining = self.producer.flush(timeout)
        if remaining > 0:
            self.logger.warning(f"{remaining} messages still pending after flush")
        else:
            self.logger.info("All messages flushed successfully")
    
    def close(self):
        self.logger.info("Closing producer...")
        self.flush()
        
        if self.metrics:
            self.metrics.print_summary()
        
        self.logger.info("Producer closed successfully")


def main():
    config = KafkaConfig.from_env()
    
    # Create producer
    producer = EnhancedProducer(config)
    
    # send multiple orders with different priorities
    orders = [
        OrderMessage(
            user="alice",
            item="laptop",
            quantity=1,
            price=1200.00,
            priority="high"
        ),
        OrderMessage(
            user="bob",
            item="keyboard",
            quantity=2,
            price=75.00,
            priority="normal"
        ),
        OrderMessage(
            user="alice",
            item="mouse",
            quantity=1,
            price=25.00,
            priority="normal"
        ),
        OrderMessage(
            user="eve",
            item="monitor",
            quantity=2,
            price=300.00,
            priority="urgent"
        ),
    ]
    
    for order in orders:
        producer.logger.info(f"Sending order: {order.order_id} - {order.quantity}x {order.item} for {order.user}")
        success = producer.send_message(order)
        if not success:
            producer.logger.error(f"Failed to send order: {order.order_id}")
        # small delay between messages is optional
        time.sleep(0.1)
    
    producer.close()


if __name__ == "__main__":
    main()
