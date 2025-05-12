from kafka import KafkaConsumer
import json
import threading
import time

def create_consumer(topic_name):
    """Create a Kafka consumer for the given topic"""
    return KafkaConsumer(
        topic_name,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

def consume_topic(topic_name):
    """Consume messages from a specific topic and print them"""
    consumer = create_consumer(topic_name)
    
    print(f"Starting consumer for {topic_name}...")
    
    try:
        for message in consumer:
            data = message.value
            
            print(f"\n[{topic_name}] New Analytics Result:")
            print("-" * 60)
            print(json.dumps(data, indent=2))
            print("-" * 60)
            
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        print(f"Consumer for {topic_name} stopped.")

def main():
    """Start consumers for all analytics topics"""
    topics = [
        "activity_analytics",
        "location_analytics",
        "purchase_analytics"
    ]
    
    # Create a thread for each topic consumer
    threads = []
    for topic in topics:
        thread = threading.Thread(target=consume_topic, args=(topic,))
        thread.daemon = True  # Allow the program to exit even if threads are running
        threads.append(thread)
        thread.start()
    
    # Keep the main thread running until interrupted
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping all consumers...")

if __name__ == "__main__":
    print("Starting Kafka Analytics Consumer...")
    print("This consumer will display results from all analytics topics.")
    print("Press Ctrl+C to exit.")
    print("-" * 60)
    main()