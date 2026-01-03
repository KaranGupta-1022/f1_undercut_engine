import json 
import logging 
import redis
from datetime import datetime
from kafka import KafkaConsumer
from strategy_engine import UndercutEngine

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers={
        logging.FileHandler("predictions.log"),
        logging.StreamHandler()
    }
)
logger = logging.getLogger(__name__)

class ConsumerWithStrategy:
    def __init__(self, bootstrap_servers="localhost:9092", redis_host="localhost", redis_port=6379, log_file="predictions.log"):
        self.consumer = KafkaConsumer(
            "f1-telemetry",
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="f1-consumer-group",
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )
        
        try: 
            self.redis_client = redis.Redis(
                host=redis_host,
                port=redis_port,
                db=0,
                decode_responses=True
            )
            self.redis_client.ping()
            logger.info(f'Connected to Redis at {redis_host}:{redis_port}')
        except Exception as e:
            logger.error(f'Failed to connect to Redis: {e}')
            self.redis_client = None
        
        self.engine = UndercutEngine()
        self.lap_count = 0
        self.check_interval = 10
        self.log_file = log_file
        self.race_session = None

        
        logger.info("=== F1 Undercut Strategy Engine Started ===")
        logger.info(f"Consumer connected to Kafka")
        logger.info(f"Predictions will be logged to {self.log_file}")
        
        # Store the current race session in Redis
        def get_sorted_drivers_by_position(self, session_name):
            self.race_session = session_name
            if self.redis_client:
                
            
        
        

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