import json 
import time
import pandas as pd
import fastf1 
from  pathlib import Path
from kafka import KafkaProducer 

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
        self.laps = self.session.laps.reset_index(drop=False)
        
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
                    "LapTime": str(lap.get("LapTime")),
                    "Compound": str(lap.get("Compound")) if lap.get("Compound") is not None else None,
                    "TyreLife": int(lap.get("TyreLife")) if "TyreLife" in lap.index and not pd.isna(lap.get("TyreLife")) else None,
                    "Stint": int(lap.get("Stint")) if "Stint" in lap.index and not pd.isna(lap.get("Stint")) else None,
                    "Position": int(lap.get("Position")) if not pd.isna(lap.get("Position")) else None,
                    "SessionName": {"EventName": getattr(self.session.event, "EventName", None)},
                    "Weather": lap.get_weather_data() if hasattr(lap, 'get_weather_data') else {},
                    "TRACK_STATUS": str(self.session.get_track_status(lap["SafetyCar"])) if "SafetyCar" in lap.index else None
                }
                unique_drivers.add(payload['Driver'])
                self.producer.send(topic, value=payload)
                print(f"Sent Lap: Driver={payload['Driver']}, LapNumber={payload['LapNumber']}, Compound={payload['Compound']}")
                sent += 1
                
                if max_laps and sent >= max_laps:
                    print(f"Reached max laps limit: {max_laps}. Stopping simulation.")
                    break
                
                if sent % 100 == 0: 
                    target = max_laps if max_laps else total_laps
                    progress = (sent / target) * 100
                    print(f"Progress: {sent} laps sent ({progress:.1f}%)")
                time.sleep(self.delay)
        except KeyboardInterrupt:
            print("Ctrl+C -> Interrupted by user.")
        finally:
            print(f"\n Summary:")
            print(f"   Total Laps Sent: {sent}")
            print(f"   Unique drivers: {len(unique_drivers)}")  # ← Add this
            print(f"   Drivers: {sorted(unique_drivers)}")
            print("Flushing producer...")
            self.producer.flush()
            self.producer.close()
            print("Producer closed.")
                
        
if __name__ == "__main__":
    sim = RaceSimulator(delay=0.1)
    sim.stream_race(max_laps=50)