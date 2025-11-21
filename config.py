import os
from typing import Optional
from pydantic import BaseModel, Field, field_validator


class KafkaConfig(BaseModel):
    bootstrap_servers: str = Field(
        default="localhost:9092",
        description="Kafka bootstrap servers"
    )
    topic_name: str = Field(
        default="orders",
        description="Main topic name"
    )
    dlq_topic_name: str = Field(
        default="orders-dlq",
        description="Dead letter queue topic name"
    )
    
    # Producer config
    producer_acks: str = Field(
        default="all",
        description="Producer acknowledgment mode"
    )
    producer_retries: int = Field(
        default=3,
        description="Number of retries for producer"
    )
    producer_compression: str = Field(
        default="snappy",
        description="Compression algorithm"
    )
    producer_linger_ms: int = Field(
        default=10,
        description="Time to wait before sending batch"
    )
    producer_batch_size: int = Field(
        default=16384,
        description="Batch size in bytes"
    )
    
    # Consumer config
    consumer_group_id: str = Field(
        default="order-processor-group",
        description="Consumer group ID"
    )
    consumer_auto_offset_reset: str = Field(
        default="earliest",
        description="Auto offset reset strategy"
    )
    consumer_enable_auto_commit: bool = Field(
        default=False,
        description="Enable auto commit"
    )
    consumer_max_poll_records: int = Field(
        default=500,
        description="Max records per poll"
    )
    consumer_session_timeout_ms: int = Field(
        default=45000,
        description="Session timeout in ms"
    )
    
    # Application specific
    max_retries: int = Field(
        default=3,
        description="Max retries for processing"
    )
    retry_backoff_ms: int = Field(
        default=1000,
        description="Backoff between retries in ms"
    )
    enable_metrics: bool = Field(
        default=True,
        description="Enable metrics collection"
    )
    log_level: str = Field(
        default="INFO",
        description="Logging level"
    )
    
    @field_validator('log_level')
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if v.upper() not in valid_levels:
            raise ValueError(f'log_level must be one of {valid_levels}')
        return v.upper()
    
    @classmethod
    def from_env(cls) -> 'KafkaConfig':
        # Load configuration from environment variables
        return cls(
            bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
            topic_name=os.getenv('KAFKA_TOPIC', 'orders'),
            dlq_topic_name=os.getenv('KAFKA_DLQ_TOPIC', 'orders-dlq'),
            producer_acks=os.getenv('KAFKA_PRODUCER_ACKS', 'all'),
            producer_retries=int(os.getenv('KAFKA_PRODUCER_RETRIES', '3')),
            producer_compression=os.getenv('KAFKA_PRODUCER_COMPRESSION', 'snappy'),
            producer_linger_ms=int(os.getenv('KAFKA_PRODUCER_LINGER_MS', '10')),
            producer_batch_size=int(os.getenv('KAFKA_PRODUCER_BATCH_SIZE', '16384')),
            consumer_group_id=os.getenv('KAFKA_CONSUMER_GROUP_ID', 'order-processor-group'),
            consumer_auto_offset_reset=os.getenv('KAFKA_CONSUMER_AUTO_OFFSET_RESET', 'earliest'),
            consumer_enable_auto_commit=os.getenv('KAFKA_CONSUMER_ENABLE_AUTO_COMMIT', 'false').lower() == 'true',
            consumer_max_poll_records=int(os.getenv('KAFKA_CONSUMER_MAX_POLL_RECORDS', '500')),
            consumer_session_timeout_ms=int(os.getenv('KAFKA_CONSUMER_SESSION_TIMEOUT_MS', '45000')),
            max_retries=int(os.getenv('MAX_RETRIES', '3')),
            retry_backoff_ms=int(os.getenv('RETRY_BACKOFF_MS', '1000')),
            enable_metrics=os.getenv('ENABLE_METRICS', 'true').lower() == 'true',
            log_level=os.getenv('LOG_LEVEL', 'INFO'),
        )
    
    def get_producer_config(self) -> dict:
        # Get producer configuration
        return {
            'bootstrap.servers': self.bootstrap_servers,
            'acks': self.producer_acks,
            'retries': self.producer_retries,
            'compression.type': self.producer_compression,
            'linger.ms': self.producer_linger_ms,
            'batch.size': self.producer_batch_size,
            'enable.idempotence': True,
            'max.in.flight.requests.per.connection': 5,
        }
    
    def get_consumer_config(self) -> dict:
        # Get consumer configuration
        return {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.consumer_group_id,
            'auto.offset.reset': self.consumer_auto_offset_reset,
            'enable.auto.commit': self.consumer_enable_auto_commit,
            'session.timeout.ms': self.consumer_session_timeout_ms,
            'enable.partition.eof': False,
        }
