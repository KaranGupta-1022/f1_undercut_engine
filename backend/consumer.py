import json 
import logging 
import redis
from datetime import datetime
from kafka import KafkaConsumer
from strategy_engine import UndercutEngine

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("predictions.log"),
        logging.StreamHandler()
    ]
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
    def set_race_session(self, session_name):
        self.race_session = session_name
        if self.redis_client:
            try:
                self.redis_client.set("f1:race:session", session_name)
                self.redis_client.expire("f1:race:session", 3600) # 1 hour expiration
                logger.info(f"Race session set in Redis: {session_name}") 
            except Exception as e:
                logger.error(f"Failed to set race session in Redis: {e}")
    
    # Cache the driver state in Redis
    def cache_driver_state(self, driver: str):
        if not self.redis_client: 
            return 
        
        try:
            state = self.engine.driver_state.get(driver)
            if state:
                cache_state = {
                    "lap_number": state['lap_number'],
                    "tyre_age": state['tyre_age'],
                    "compound": state['compound'],
                    "position": state['position'],
                    "current_pace": state['current_pace'],
                    "stint": state.get('stint'),
                    "last_lap_time": state["lap_times"][-1] if state["lap_times"] else None
                }
                
                self.redis_client.hset(f"f1:driver:{driver}", mapping=cache_state)
                self.redis_client.expire(f"f1:driver:{driver}", 3600) # 1 hour expiration
                
        except Exception as e:
            logger.error(f"Failed to cache driver state for {driver}: {e}")
            
    # Cache prediction results in Redis
    def cache_prediction(self, ahead: str, behind: str, result: dict):
        if not self.redis_client:
            return 
        
        try:
            cache_key = f"f1:prediction:{ahead}:{behind}:{self.lap_count}"
            prediction_data = {
                "timestamp": datetime.now().isoformat(),
                "viable": str(result['viable']),
                "time_delta": str(result['time_delta']),
                "confidence": str(result['confidence']),
                "recommendation": result['recommendation'],
                "pit_loss": str(result['pit_loss']),
                "track_status": result['track_status']
            }
            
            self.redis_client.hset(cache_key, mapping=prediction_data)
            self.redis_client.expire(cache_key, 3600)  # 1 hour expiry
            
            # Also store in a sorted set for quick retrieval of recent predictions
            self.redis_client.zadd(
                'f1:predictions:recent',
                {f'{ahead}:{behind}': self.lap_count}
            )
            
        except Exception as e:
            logger.error(f"Failed to cache prediction: {e}")
            
            
    # Return drivers sorted by current postion
    def get_sorted_drivers_by_position(self):
        drivers = self.engine.get_all_drivers()
        driver_positions = []
        
        for driver in drivers:
            state = self.engine.driver_state.get(driver)
            if state:
                position = state.get("position", 999)
                driver_positions.append((driver, position))
                
        # Sort by position in ascending order
        driver_positions.sort(key=lambda x: x[1])
        return [driver for _, driver in driver_positions]
    
    # Check for undercut between consecutive drivers
    def check_undercut_opportunities(self):
        sorted_drivers = self.get_sorted_drivers_by_position()
        
        if len(sorted_drivers) < 2:
            return
        
        logger.info(f"\n{'='*80}")
        logger.info(f"UNDERCUT WINDOW CHECK - Lap {self.lap_count}")
        logger.info(f"Drivers (sorted by position): {sorted_drivers}")
        logger.info(f"{'='*80}\n")
        
        for i in range(len(sorted_drivers) - 1):
            ahead = sorted_drivers[i]
            behind = sorted_drivers[i + 1]
            
            result = self.engine.predict_undercut_window(ahead, behind)
            
            if result is None:
                continue
            
            # Cache and log the prediction
            self.cache_prediction(ahead, behind, result)
            self.log_prediction(ahead, behind, result)
            
            if result['viable']:
                self.print_alert(ahead, behind, result)
                
    # Log prediction results to file and Redis
    def log_prediction(self, ahead: str, behind: str, result: dict):
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "ahead": ahead,
            "behind": behind,
            "viable": result["viable"],
            "time_delta": result["time_delta"],
            "confidence": result["confidence"],
            "recommendation": result["recommendation"],
            "ahead_projected": result["ahead_projected"],
            "behind_projected": result["behind_projected"],
            "pit_loss": result["pit_loss"],
            "ahead_tire_age": result["ahead_tire_age"],
            "ahead_compound": result["ahead_compound"],
            "track_status": result["track_status"]
        }
        
        logger.info(f"[{ahead} vs {behind}] {json.dumps(log_entry, indent=2)}")
        
    # Print formatted undercut opportunity alert
    def print_alert(self, ahead: str, behind: str, result: dict):
        alert = f"""
                ╔════════════════════════════════════════════════════════════════╗
                ║                    🏁 UNDERCUT DETECTED! 🏁                    ║
                ╠════════════════════════════════════════════════════════════════╣
                ║ Driver AHEAD (to undercut):   {ahead:40} ║
                ║ Driver BEHIND (pitting):      {behind:40} ║
                ║ Recommendation:               {result['recommendation']:40} ║
                ╠════════════════════════════════════════════════════════════════╣
                ║ Time Delta:                   {result['time_delta']:6}s gain              ║
                ║ Confidence:                   {result['confidence'] * 100:5.0f}%                  ║
                ║ Pit Loss:                     {result['pit_loss']:6.1f}s                   ║
                ║                                                                ║
                ║ {ahead}'s Projected Pace:     {result['ahead_projected']:6.2f}s (tire age: {result['ahead_tire_age']}) ║
                ║ {behind}'s Projected Pace:    {result['behind_projected']:6.2f}s (after pit)      ║
                ║                                                                ║
                ║ Tire Info:                                                     ║
                ║   {ahead} ({result['ahead_compound']:6}):  {result['ahead_degradation']:.4f}s/lap degradation        ║
                ║   {behind} ({result['behind_compound']:6}):  compound advantage: {result['compound_advantage']:+.2f}s ║
                ║                                                                ║
                ║ Conditions: {result['weather_condition']:20} | Status: {result['track_status']:10} ║
                ║ Reason: {result['reason']:50} ║
                ╚════════════════════════════════════════════════════════════════╝
                """
        logger.info(alert)
        print(alert)
        
    def consume(self):
        logger.info("Listening for messages on topic 'f1-telemetry' topic...")
        
        try:
            for message in self.consumer:
                data = message.value

                # Get the driver and update the engine
                driver = data.get("Driver")
                if not driver:
                    continue 
                
                # Set race session from first message
                if not self.race_session:
                    session_name = data.get("SessionName", {}).get("EventName", "Unknown")
                    self.set_race_session(session_name)
                    
                # Map Kafka data to strategy engine format
                lap_data = {
                    "LapNumber": data.get("LapNumber"),
                    "LapTime": data.get("LapTime"),
                    "Compound": data.get("Compound"),
                    "TyreLife": data.get("TyreLife"),
                    "Position": data.get("Position"),
                    "Stint": data.get("Stint"),
                    "Weather": data.get("Weather", {}),
                    "TRACK_STATUS": data.get("TrackStatus", "1")
                }
                
                self.engine.update_driver_state(driver, lap_data)
                self.lap_count += 1
                
                # Cache driver state in Redis
                self.cache_driver_state(driver)
                
                # Print lap details
                lap_num = data.get("LapNumber", "N/A")
                compound = data.get("Compound", "N/A")
                tyre_life = data.get("TyreLife", "N/A")
                position = data.get("Position", "N/A")
                
                print(f"[LAP {self.lap_count}] {driver:4} | Lap {lap_num:3} | {compound:6} | TyreLife {tyre_life:2} | Pos {position}")

                # Check for undercut at N laps
                if self.lap_count % self.check_interval == 0:
                    self.check_undercut_opportunities()
                    
        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        except Exception as e:
            logger.info(f"Consumer error: {e}", exc_info=True)
        finally:
            self.consumer.close()
            logger.info("Consumer closed.")
            if self.redis_client:
                try:
                    self.redis_client.close()
                    logger.info("Redis connection closed.")
                except Exception as e:
                    logger.error(f"Error closing Redis connection: {e}")    
            self.print_summary()
            
    # Print a summary of driver states
    def print_summary(self):
        if self.redis_client:
            try: 
                predictions_count = self.redis_client.zcard("f1:predictions:recent")
                logger.info(f"\nRedis cached predictions: {predictions_count}")
                logger.info(f"Race session stored: {self.redis_client.get('f1:race:session')}")
            except Exception as e:
                logger.error(f"Failed to retrieve summary from Redis: {e}")
            
        logger.info("="*80)
        logger.info(f"All predictions logged to {self.log_file}")
        
def main():
    consumer = ConsumerWithStrategy()
    consumer.consume()
      
if __name__ == '__main__':
    main()