import json
import sys
from config import KafkaConfig
from consumer import EnhancedConsumer


def check_consumer_health():
    try:
        config = KafkaConfig.from_env()
        consumer = EnhancedConsumer(config)
        
        health = consumer.health_check()
        
        print("Consumer Health Check")
        print("=" * 50)
        print(f"Status: {health['status']}")
        print(f"Running: {health['running']}")
        print(f"Circuit Breaker: {health['circuit_breaker_state']}")
        
        if health.get('metrics'):
            print("\nMetrics:")
            print(json.dumps(health['metrics'], indent=2))
        
        sys.exit(0 if health['status'] == 'healthy' else 1)
        
    except Exception as e:
        print(f"Health check failed: {e}")
        sys.exit(1)


def print_metrics_summary():
    from utils import MetricsCollector
    
    metrics = MetricsCollector()
    
    # Simulate some metrics
    metrics.increment('test.counter', 100)
    metrics.record_timing('test.latency', 45.5)
    metrics.record_timing('test.latency', 52.3)
    metrics.record_timing('test.latency', 48.1)
    
    metrics.print_summary()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "metrics":
        print_metrics_summary()
    else:
        check_consumer_health()
