import json 
import time
import pandas as pd
import fastf1 
from kafka import KafkaProducer 

fastf1.Cache.enable_cache("cache")

class RaceSimulator:
    def __init__(self, year=2024, event="Monaco", session_type="R", bootstrap_servers="localhost:9092", delay=0.5):
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
                    "SessionName": {"EventName": getattr(self.session.event, "EventName", None)}
                }
                
                self.producer.send(topic, value=payload)
                print(f"Sent Lap: Driver={payload['Driver']}, LapNumber={payload['LapNumber']}, Compound={payload['Compound']}")
                sent += 1
                if max_laps and sent >= max_laps:
                    print(f"Reached max laps limit: {max_laps}. Stopping simulation.")
                    break
                time.sleep(self.delay)
        except KeyboardInterrupt:
            print("Ctrl+C -> Interrupted by user.")
        finally:
            print("Flushing producer...")
            self.producer.flush()
            self.producer.close()
            print("Producer closed.")
                
        
if __name__ == "__main__":
    sim = RaceSimulator(delay=0.3)
    sim.stream_race(max_laps=50)