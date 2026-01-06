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
        logging.FileHandler("predictions.log",  mode='w', encoding='utf-8'),  # ← Add encoding
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
        self.message_count = 0  # Total messages received
        self.current_race_lap = 0  # Current lap number in the race
        self.check_interval = 5  # Check every 5 laps
        self.log_file = log_file
        self.race_session = None
        self.last_checked_lap = 0  # Track when we last checked for undercuts
        self.lap_count = 0
        self.session_loaded = False
        
        logger.info("=== F1 Undercut Strategy Engine Started ===")
        logger.info(f"Consumer connected to Kafka")
        logger.info(f"Predictions will be logged to {self.log_file}")
      
    # Extract track name from session name (e.g., "Monaco Grand Prix" -> "Monaco")  
    def handle_session_info(self, session_name: str):
        if session_name:
            self.engine.set_track(session_name)
            logger.info(f"Engine configured for track: {session_name}")
            logger.info(f"Track config: {self.engine.get_track_config()}")
        
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
                    "lap_number": str(state['lap_number']),
                    "tyre_age": str(state['tyre_age']),
                    "compound": state['compound'] or "UNKNOWN",
                    "position": str(state['position']) if state['position'] else "0",
                    "current_pace": str(state['current_pace']) if state['current_pace'] else "0",
                    "stint": str(state.get('stint')) if state.get('stint') else "0",
                    "last_lap_time": str(state["lap_times"][-1]) if state["lap_times"] else "0"
                }
                
                self.redis_client.hset(f"f1:driver:{driver}", mapping=cache_state)
                self.redis_client.expire(f"f1:driver:{driver}", 3600)
                
        except Exception as e:
            logger.error(f"Failed to cache driver state for {driver}: {e}")
            
    # Cache prediction results in Redis
    def cache_prediction(self, ahead: str, behind: str, result: dict):
        if not self.redis_client:
            return 
        
        try:
            cache_key = f"f1:prediction:{ahead}:{behind}:{self.current_race_lap}"
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
                {f'{ahead}:{behind}': self.current_race_lap}
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
                position = state.get("position")
                
                # Handle None positions (retired/DNF drivers)
                # Treat None as 999 so they sort to the end
                if position is None:
                    position = 999
                    
                driver_positions.append((driver, position))
        
        # Filter out drivers with no valid position data
        # (those still at 999 - haven't started or retired)
        valid_positions = [(d, p) for d, p in driver_positions if p < 900]
        
        # Sort by position (second element of tuple) in ascending order
        valid_positions.sort(key=lambda x: x[1])
        
        return [driver for driver, _ in valid_positions]
    
    # Check for undercut between consecutive driver
    def check_undercut_opportunities(self):
        sorted_drivers = self.get_sorted_drivers_by_position()
        
        if len(sorted_drivers) < 2:
            return
        
        logger.info(f"\n{'='*80}")
        logger.info(f"UNDERCUT WINDOW CHECK - Race Lap {self.current_race_lap}")
        logger.info(f"Total Messages Processed: {self.message_count}")
        logger.info(f"Drivers (sorted by position): {sorted_drivers}")
        logger.info(f"{'='*80}\n")
        
        comparisons_made = 0
        lapped_cars_skipped = 0
        
        for i in range(len(sorted_drivers) - 1):
            ahead = sorted_drivers[i]
            behind = sorted_drivers[i + 1]
            
            # Get lap numbers for both drivers
            ahead_state = self.engine.driver_state.get(ahead)
            behind_state = self.engine.driver_state.get(behind)
            
            if not ahead_state or not behind_state:
                continue
            
            ahead_lap = ahead_state.get('lap_number', 0)
            behind_lap = behind_state.get('lap_number', 0)
            
            # LAPPED CAR HANDLING: Only compare if on same lap or within 1 lap
            lap_difference = abs(ahead_lap - behind_lap)
            
            if lap_difference > 1:
                logger.debug(f"Skipping {ahead} (Lap {ahead_lap}) vs {behind} (Lap {behind_lap}) - "
                           f"lap difference too large ({lap_difference} laps)")
                lapped_cars_skipped += 1
                continue
            
            # If exactly 1 lap apart, note it but still analyze
            if lap_difference == 1:
                logger.info(f"⚠️  Analyzing {ahead} vs {behind} despite 1 lap difference")
            
            result = self.engine.predict_undercut_window(ahead, behind)
            
            if result is None:
                continue
            
            comparisons_made += 1
            
            # Cache and log the prediction
            self.cache_prediction(ahead, behind, result)
            self.log_prediction(ahead, behind, result)
            
            if result['viable']:
                self.print_alert(ahead, behind, result)
        
        logger.info(f"\nUndercut Analysis Summary:")
        logger.info(f"  - Comparisons made: {comparisons_made}")
        logger.info(f"  - Lapped cars skipped: {lapped_cars_skipped}")

                
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
        # alert = f"""
        # ╔══════════════════════════════════════════════════════════════╗
        # ║                     UNDERCUT OPPORTUNITY!                   ║ 
        # ╠══════════════════════════════════════════════════════════════╣
        # ║ Driver Ahead : {ahead:4}                                      ║
        # ║ Driver Behind: {behind:4}                                      ║
        # ╠══════════════════════════════════════════════════════════════╣
        # ║ Viable Undercut : YES                                       ║
        # ║ Time Delta/Lap  : {result['time_delta']} seconds            ║
        # ║ Current Gap     : {result.get('current_gap', 'N/A')} seconds ║
        # ║ Laps to Overcome: {result.get('laps_to_overcome', 'N/A')} laps ║
        # ║ Confidence      : {result['confidence']} %                  ║
        # ║ Pit Stop Loss   : {result['pit_loss']} seconds             ║
        # ║ Recommendation  : {result['recommendation']}                 ║
        # ╚══════════════════════════════════════════════════════════════╝
        #         """
        alert = f"""
        {'='*80}
                            *** UNDERCUT DETECTED! ***
        {'='*80}
        Driver AHEAD (to undercut):   {ahead}
        Driver BEHIND (pitting):      {behind}
        Recommendation:               {result['recommendation']}
        {'='*80}
        Current Gap:                  {result['current_gap']:.2f}s
        Time Delta (per lap):         {result['time_delta']:.2f}s gain
        Laps to Overcome Deficit:     {result['laps_to_overcome']}
        Confidence:                   {result['confidence'] * 100:.0f}%
        Pit Loss:                     {result['pit_loss']:.1f}s
                """
        logger.info(alert)
        print(alert)
        
    def consume(self):
        logger.info("Listening for messages on topic 'f1-telemetry' topic...")
        
        try:
            for message in self.consumer:
                data = message.value
                
                # Load session config on first message
                if not self.session_loaded:
                    session_info = data.get('SessionName', {})
                    if isinstance(session_info, dict):
                        event_name = session_info.get('EventName')
                    else:
                        event_name = str(session_info)
                    if event_name:
                        self.handle_session_info(event_name)
                    self.session_loaded = True

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
                self.message_count += 1
                
                # Update current race lap
                lap_num = data.get("LapNumber", 0)
                if lap_num and lap_num > self.current_race_lap:
                    self.current_race_lap = lap_num
                
                # Cache driver state in Redis
                self.cache_driver_state(driver)
                
                # Print lap details
                compound = data.get("Compound", "N/A")
                tyre_life = data.get("TyreLife", "N/A")
                position = data.get("Position", "N/A")
                
                print(f"[MSG {self.message_count:4}] Lap {lap_num:3} | {driver:4} | {compound:6} | Tire {tyre_life:2} | P{position}")

                # Check for undercut every N laps (not every N messages!)
                if (self.current_race_lap > 0 and 
                    self.current_race_lap % self.check_interval == 0 and
                    self.current_race_lap != self.last_checked_lap):
                    self.check_undercut_opportunities()
                    self.last_checked_lap = self.current_race_lap
                    
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
        logger.info("\n" + "="*80)
        logger.info("CONSUMER SUMMARY")
        logger.info("="*80)
        logger.info(f"Total Messages Processed: {self.message_count}")
        logger.info(f"Total Race Laps Covered: {self.current_race_lap}")
        logger.info(f"Unique Drivers Tracked: {len(self.engine.get_all_drivers())}")
        logger.info(f"Drivers: {', '.join(sorted(self.engine.get_all_drivers()))}")
        
        if self.redis_client:
            try: 
                predictions_count = self.redis_client.zcard("f1:predictions:recent")
                logger.info(f"Redis Cached Predictions: {predictions_count}")
                logger.info(f"Race Session: {self.redis_client.get('f1:race:session')}")
            except Exception as e:
                logger.error(f"Failed to retrieve summary from Redis: {e}")
            
        logger.info("="*80)
        logger.info(f"All predictions logged to {self.log_file}")
        
def main():
    consumer = ConsumerWithStrategy()
    consumer.consume()
      
if __name__ == '__main__':
    main()