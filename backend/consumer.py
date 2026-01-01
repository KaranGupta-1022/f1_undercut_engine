import json 
from kafka import KafkaConsumer

def main():
    consumer = KafkaConsumer(
        "f1-telemetry",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="f1-consumer-group",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

    print("Listening for messages on topic 'f1-telemetry' (Ctrl+C to exit)...")
    try:
        for message in consumer:
            print("-----")
            print(f"Partition: {message.partition}, Offset: {message.offset}")
            print(f'Value: {message.value}')
    except KeyboardInterrupt:
        print("Interrupted by user")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        consumer.close()
        print("Consumer closed.")
           
    
if __name__ == "__main__":
    main()