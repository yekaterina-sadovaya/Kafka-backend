import time
from config import KafkaConfig
from producer import EnhancedProducer
from consumer import EnhancedConsumer
from schemas import OrderMessage


def priority_processor(message: OrderMessage) -> bool:
    priority_delays = {
        'urgent': 0.01,
        'high': 0.05,
        'normal': 0.1,
        'low': 0.5
    }
    
    # Simulate priority-based processing
    delay = priority_delays.get(message.priority, 0.1)
    time.sleep(delay)
    
    print(f"⚡ [{message.priority.upper()}] Processed order {message.order_id}")
    return True


def run_producer_example():
    config = KafkaConfig.from_env()
    producer = EnhancedProducer(config)
    
    # Example 1: High-priority order
    urgent_order = OrderMessage(
        user="vip_customer",
        item="premium_item",
        quantity=1,
        price=999.99,
        priority="urgent"
    )
    producer.send_message(urgent_order)
    
    # Example 2: Bulk orders from same user (same partition)
    for i in range(5):
        order = OrderMessage(
            user="bulk_buyer",
            item=f"item_{i}",
            quantity=10,
            price=50.0,
            priority="normal"
        )
        producer.send_message(order)
    
    # Example 3: Order with custom headers
    order_with_headers = OrderMessage(
        user="tagged_customer",
        item="tracked_item",
        quantity=3,
        price=75.0
    )
    producer.send_message(
        order_with_headers,
        headers={
            'region': 'us-west',
            'campaign_id': 'summer-2025',
            'source': 'mobile-app'
        }
    )
    
    producer.close()


def run_consumer_example():
    config = KafkaConfig.from_env()
    
    consumer = EnhancedConsumer(
        config,
        message_processor=priority_processor
    )
    
    consumer.start()


def run_multi_topic_example():
    config = KafkaConfig.from_env()
    producer = EnhancedProducer(config)
    
    # Override topic for specific messages
    original_topic = config.topic_name
    
    # High priority orders go to priority topic
    config.topic_name = "orders-priority"
    priority_order = OrderMessage(
        user="vip",
        item="exclusive_item",
        quantity=1,
        priority="urgent"
    )
    producer.send_message(priority_order)
    
    # Regular orders go to normal topic
    config.topic_name = original_topic
    regular_order = OrderMessage(
        user="regular_user",
        item="standard_item",
        quantity=5,
        priority="normal"
    )
    producer.send_message(regular_order)
    
    producer.close()


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python examples.py [producer|consumer|multi-topic]")
        sys.exit(1)
    
    mode = sys.argv[1]
    
    if mode == "producer":
        run_producer_example()
    elif mode == "consumer":
        run_consumer_example()
    elif mode == "multi-topic":
        run_multi_topic_example()
    else:
        print(f"Unknown mode: {mode}")
        sys.exit(1)
