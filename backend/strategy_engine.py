from datetime import timedelta
from typing import Dict, Optional, List

class UndercutEngine:
    # Tire degradation rates for each compound (seconds per lap)
    DEGRADATION_RATES = {
        "Soft": 1.5,
        "Medium": 1.0,
        "Hard": 0.7
    }
    
    # Pit stop times (in seconds) 
    PIT_LOSS = 20.0 
    SAFETY_CAR_PIT_LOSS = 10.0 
    FRESH_TIRE_ADVANTAGE = 1.5 # seconds gained per lap on fresh tires
    AMORTIZATION_LAPS = 3  # Laps over which fresh tire advantage is amortized 
    SAFETY_CAR_THRESHOLD = 5.0 # seconds gap threshold to consider pitting under safety car    
    
    # Weather impact on tire degradation
    WEATHER_IMPACTS = {
        "rain": 0.5,
        "dry": 1.0,
        "hot": 1.2,
        "cool": 0.9
    }
    
    # Track status codes
    TRACK_STATUS_CODES = {
        "1": "GREEN",
        "2": "YELLOW",
        "4": "SAFETY_CAR",
        "5": "RED",
        "6": "VSC Deployed"
    }
    
    def __init__(self):
        # driver_state[driver_name] = {
        #   "lap_number": int,
        #   "tyre_age": int,
        #   "compound": str,
        #   "position": int,
        #   "lap_times": [float, ...],
        #   "current_pace": float,
        #   "weather": {...},  # current weather conditions
        #   "track_status": str,  # current track status
        #   "stint": int,
        #   "gap_to_leader": float
        # }
        self.driver_state = {}
        self.weather = {}
        self.track_status = "GREEN"

    # Convert timedelta or string to seconds 
    def lap_time_to_seconds(self, lap_time)-> Optional[float]:
        if lap_time is None:
            return None
        if isinstance(lap_time, timedelta):
            return lap_time.total_seconds()
        if isinstance(lap_time, str):
            try:
                parts = lap_time.split(":")
                if len(parts) == 3:
                    hours, minutes, seconds = parts
                    return int(hours) * 3600 + int(minutes) * 60 + float(seconds)
                elif len(parts) == 2:
                    minutes, seconds = parts
                    return int(minutes) * 60 + float(seconds)
                else:
                    return float(lap_time)
            except:
                return None
        return float(lap_time)
    
    # Parase and normalize weather data
    def parse_weather_data(self, weather_dict: dict) -> dict:
        if not weather_dict:
            return {}
        
        parsed = {
            "airtemp": weather_dict.get("AirTemp"),
            "humidity": weather_dict.get("Humidity"),
            "pressure": weather_dict.get("Pressure"),
            "rainfall": weather_dict.get("Rainfall"),
            "tracktemp": weather_dict.get("TrackTemp"),
            "winddirection": weather_dict.get("WindDirection"),
            "windspeed": weather_dict.get("WindSpeed"),
        }
        return parsed

    # Determine weather condition from weather data
    def get_weather_condition(self, weather_dict: Dict) -> str:
        if not weather_dict:
            return "dry"
        
        rainfall = weather_dict.get("Rainfall", False)
        if rainfall: 
            return "rain"
        
        track_temp = weather_dict.get("TrackTemp")
        if track_temp and track_temp >= 40:
            return "hot"
        elif track_temp and track_temp <= 30:
            return "cool"
        
        return "dry"
    
    # Get tire degradiction rate based on weather
    def get_degradation_multiplier(self, weather_dict: dict) -> float:
        condition = self.get_weather_condition(weather_dict)
        return self.WEATHER_IMPACTS.get(condition, 1.0)
    
    # Check if safety car is active
    def is_safety_car_active(self) -> bool:
        return self.track_status in ["SAFETY_CAR", "VSC Deployed"]
    
    # Update track status from code
    def update_track_status(self, track_status_code: str):
        self.track_status = self.TRACK_STATUS_CODES.get(track_status_code, "GREEN")
        
    # Update driver state from lap data
    def update_driver_state(self, driver: str, lap_data: dict):
        lap_number = lap_data.get("LapNumber")
        compound = lap_data.get("Compound")
        tyre_life = lap_data.get("TyreLife")
        position = lap_data.get("Position")
        lap_time = lap_data.get("LapTime")
        stint = lap_data.get("Stint")
        weather = lap_data.get("Weather", {})
        track_status = lap_data.get("TRACK_STATUS", "GREEN")
        
        # Update global track status
        if track_status and track_status != "GREEN":
            self.update_track_status(track_status)
            
        if driver not in self.driver_state:
            self.driver_state[driver] = {
                "lap_number": 0,
                "tyre_age": 0,
                "compound": None,
                "position": position,
                "lap_times": [],
                "current_pace": 0.0,
                "weather": {},
                "track_status": "GREEN",
                "stint": 1,
                "gap_to_leader": 0.0
            }
            
        # Update driver state
        state = self.driver_state[driver]
        state["lap_number"] = lap_number if lap_number else state["lap_number"]
        state["position"] = position if position else state["position"]
        state["compound"] = compound if compound else state["compound"]
        state["track_status"] = track_status if track_status else state["track_status"]
        state["stint"] = stint if stint else state["stint"]
        
        # Parse and store weather data
        if weather:
            state["weather"] = self.parse_weather_data(weather)
            
        # Tire age 
        if tyre_life is not None:
            state["tyre_age"] = tyre_life
            
        # Add lap times 
        lap_seconds = self.lap_time_to_seconds(lap_time)
        if lap_seconds and lap_seconds > 0:
            state["lap_times"].append(lap_seconds)
            if len(state["lap_times"]) > 10:
                state["lap_times"] = state["lap_times"][-10:]
            
            # Calculate current pace as average of last 3 laps
            recent_laps = state["lap_times"][-3:]
            state["current_pace"] = sum(recent_laps) / len(recent_laps)
            
    # Calculate projected pace for a driver (Accounting for tire degradation and waeather)
    def calculate_projected_pace(self, driver: str, future_laps: int = 1) -> Optional[float]:
        if driver not in self.driver_state:
            return 0.0
        
        state = self.driver_state[driver]
        if not state["lap_times"]:
            return 0.0
    
        current_pace = state["current_pace"]
        degradation_rate = self.DEGRADATION_RATES.get(state["compound"], 0.05)
        weather_multiplier = self.get_degradation_multiplier(state["weather"])
        adjusted_degradation = degradation_rate * weather_multiplier
        
        # Projected pace calculation
        tire_age = state["tyre_age"]
        projected_pace = current_pace
        for lap in range(future_laps):
            tire_wear = tire_age + lap
            degradation_penalty = adjusted_degradation * tire_wear
            projected_pace += degradation_penalty
        
        return projected_pace / future_laps
    
    # Predict if an undercut is possible
    # Consider normal racing conditions,safety car, and the weather
    # returns dict with undercut recommendation and details
    # viable (bool): is undercut viable
    # time delta (float): estimated time gain from undercut
    # confidence (float): 0.0 - 1.0
    # reason  (str): explanation 
    # pit_loss (float): estimated pit stop loss time
    def predict_undercut_window(self, driver_ahead: str, driver_behind: str) -> Optional[dict]:
        