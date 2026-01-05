import json 
import time
import pandas as pd
import fastf1 
import logging
from  pathlib import Path
from kafka import KafkaProducer 

logger = logging.getLogger(__name__)
project_root = Path(__file__).resolve().parent.parent
cache_dir = project_root / "cache"
cache_dir.mkdir(parents=True, exist_ok=True)
fastf1.Cache.enable_cache(str(cache_dir))

class RaceSimulator:
    def __init__(self, year=2024, event="Monaco", session_type="R", bootstrap_servers="localhost:9092", delay=1.0):
        self.delay = delay
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            retries=5
        )
        print(f"Loading session: {year} {event} {session_type}")
        self.session = fastf1.get_session(year, event, session_type)
        self.session.load()
        
        # Sort laps by LapNumber first, then by driver (for consistent ordering)
        self.laps = self.session.laps.sort_values(['LapNumber', 'Driver']).reset_index(drop=True)
        
        print(f"Loaded {len(self.laps)} total laps")
        print(f"Unique drivers: {self.laps['Driver'].nunique()}")
        print(f"Lap range: {self.laps['LapNumber'].min()} - {self.laps['LapNumber'].max()}")

    # Convert timedelta to total seconds.    
    def timedelta_to_seconds(self, td):
        if td is None:
            return None
        if isinstance(td, float) or isinstance(td, int):
            return float(td)
        try:
            if hasattr(td, 'total_seconds'):
                return td.total_seconds()
            # Handle string format "0 days HH:MM:SS.ffffff"
            if isinstance(td, str) and 'days' in td:
                parts = td.split()
                if len(parts) >= 3:
                    days = int(parts[0])
                    time_str = parts[2]
                    time_parts = time_str.split(':')
                    hours = int(time_parts[0])
                    minutes = int(time_parts[1])
                    seconds = float(time_parts[2])
                    return days * 86400 + hours * 3600 + minutes * 60 + seconds
        except:
            pass
        return None
        
    # Convert weather data into dict
    def get_weather_dict(self, lap):
        try:
            # Get weather data for this lap
            weather_data = lap.get_weather_data() if hasattr(lap, 'get_weather_data') else None
            
            if weather_data is None:
                return {}
            
            # If it's a pandas Series, convert to dict
            if hasattr(weather_data, 'to_dict'):
                return weather_data.to_dict()
            
            # If already a dict, return it
            if isinstance(weather_data, dict):
                return weather_data
            
            # Otherwise return empty dict
            return {}
        except Exception as e:
            logger.warning(f"Error parsing weather data: {e}")
            return {}
        
    # Get track status code for a specific lap's timestamp.
    def get_track_status_for_lap(self, lap):
        try:
            lap_time = lap.get('Time')
            if lap_time is None:
                return "1"  # default to green
            
            # FastF1 session.track_status is a DataFrame with Time and Status columns
            track_status_df = self.session.track_status
            if track_status_df is None or track_status_df.empty:
                return "1"
            
            # Find the status that was active at lap time
            # Status changes throughout the race; find the last one before this lap
            relevant_statuses = track_status_df[track_status_df['Time'] <= lap_time]
            
            if relevant_statuses.empty:
                return "1"
            
            # Get the most recent status before this lap
            latest_status = relevant_statuses.iloc[-1]
            status_code = str(latest_status['Status'])
            
            return status_code
        except Exception as e:
            logger.error(f"Error getting track status: {e}")
            return "1"
        
    def stream_race(self, topic="f1-telemetry", max_laps=None):
        sent = 0
        unique_drivers = set()
        total_laps = len(self.laps)
        print(f"Total laps to stream: {total_laps}")
        
        try:
            for _, lap in self.laps.iterrows():
                payload = {
                    "Driver": str(lap["Driver"]),
                    "LapNumber": int(lap.get("LapNumber")) if not pd.isna(lap.get("LapNumber")) else None,
                    "LapTime": self.timedelta_to_seconds(lap.get("LapTime")),
                    "Compound": str(lap.get("Compound")) if lap.get("Compound") is not None else None,
                    "TyreLife": int(lap.get("TyreLife")) if "TyreLife" in lap.index and not pd.isna(lap.get("TyreLife")) else None,
                    "Stint": int(lap.get("Stint")) if "Stint" in lap.index and not pd.isna(lap.get("Stint")) else None,
                    "Position": int(lap.get("Position")) if not pd.isna(lap.get("Position")) else None,
                    "SessionName": {"EventName": getattr(self.session.event, "EventName", None)},
                    "Weather": self.get_weather_dict(lap),
                    "TrackStatus": self.get_track_status_for_lap(lap)
                }
                unique_drivers.add(payload['Driver'])
                self.producer.send(topic, value=payload)
                print("Sent payload:")
                print(json.dumps(payload, indent=2, default=str))
                sent += 1
                
                if max_laps and sent >= max_laps:
                    print(f"Reached max laps limit: {max_laps}. Stopping simulation.")
                    break
                
                if sent % 100 == 0: 
                    current_lap = payload['LapNumber']
                    target = max_laps if max_laps else total_laps
                    progress = (sent / target) * 100
                    print(f"Progress: {sent} messages sent ({progress:.1f}%) - Currently streaming Lap {current_lap}")
                time.sleep(self.delay)
        except KeyboardInterrupt:
            print("Ctrl+C -> Interrupted by user.")
        finally:
            print(f"\n═══════════════════════════════════════")
            print(f"   RACE SIMULATION SUMMARY")
            print(f"═══════════════════════════════════════")
            print(f"   Total Messages Sent: {sent}")
            print(f"   Unique Drivers: {len(unique_drivers)}")
            print(f"   Drivers: {', '.join(sorted(unique_drivers))}")
            if sent > 0:
                approx_laps = sent // len(unique_drivers) if unique_drivers else 0
                print(f"   Approximate Laps Simulated: {approx_laps}")
            print(f"═══════════════════════════════════════")
            print("Flushing producer...")
            self.producer.flush()
            self.producer.close()
            print("Producer closed.")
                
        
if __name__ == "__main__":
    sim = RaceSimulator(delay=0.1)
    sim.stream_race()