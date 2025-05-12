from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

# Create a Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Topic name
TOPIC_NAME = "user_activities"

# Function to generate random user activity data
def generate_user_activity():
    user_ids = list(range(1, 11))
    activities = ["login", "logout", "purchase", "page_view", "click", "search"]
    products = ["laptop", "phone", "tablet", "headphones", "keyboard", "mouse", "monitor"]
    locations = ["New York", "San Francisco", "Chicago", "Miami", "Seattle", "Austin", "Boston"]
    
    return {
        "user_id": random.choice(user_ids),
        "activity_type": random.choice(activities),
        "timestamp": datetime.now().isoformat(),
        "details": {
            "product": random.choice(products) if random.random() > 0.5 else None,
            "location": random.choice(locations),
            "duration_seconds": random.randint(5, 300),
            "value": round(random.uniform(1.0, 500.0), 2) if random.random() > 0.7 else None
        }
    }

# Main function to continuously produce messages
def main():
    try:
        while True:
            # Generate a user activity
            activity_data = generate_user_activity()
            
            # Send the data to Kafka
            producer.send(TOPIC_NAME, activity_data)
            
            # Print what was sent (for debugging)
            print(f"Sent: {json.dumps(activity_data, indent=2)}")
            
            # Flush to ensure message is sent
            producer.flush()
            
            # Wait a bit before sending the next message
            time.sleep(2)
    except KeyboardInterrupt:
        print("Producer stopped.")
    finally:
        producer.close()

if __name__ == "__main__":
    print(f"Starting to produce messages to topic '{TOPIC_NAME}'...")
    main()