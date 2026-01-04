from datetime import timedelta
from typing import Dict, Optional, List

class UndercutEngine:
    # Tire degradation rates for each compound (seconds per lap)
    DEGRADATION_RATES = {
        "SOFT": 1.5,
        "MEDIUM": 1.0,
        "HARD": 0.7
    }
    
    # Tire compound pace advantage (seconds per lap)
    # How much faster one compound is vs another
    COMPOUND_ADVANTAGE = {
        ("SOFT", "MEDIUM"): 0.5,   # SOFT is 0.5s/lap faster than MEDIUM
        ("SOFT", "HARD"): 0.8,     # SOFT is 0.8s/lap faster than HARD
        ("MEDIUM", "HARD"): 0.3,   # MEDIUM is 0.3s/lap faster than HARD
        ("MEDIUM", "SOFT"): -0.5,  # MEDIUM is 0.5s/lap slower than SOFT
        ("HARD", "SOFT"): -0.8,    # HARD is 0.8s/lap slower than SOFT
        ("HARD", "MEDIUM"): -0.3,  # HARD is 0.3s/lap slower than MEDIUM
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
                # Handle "0 days HH:MM:SS.ffffff" format from timedelta
                if 'days' in lap_time:
                    parts = lap_time.split()
                    if len(parts) >= 3:
                        days = int(parts[0])
                        time_str = parts[2]
                        time_parts = time_str.split(':')
                        hours = int(time_parts[0])
                        minutes = int(time_parts[1])
                        seconds = float(time_parts[2])
                        return days * 86400 + hours * 3600 + minutes * 60 + seconds
                
                # Handle "MM:SS.fff" format
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
        # Handle non-dict input (e.g., string, None, etc.)
        if not isinstance(weather_dict, dict):
            return {}
        
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

        # Accept both lowercase and capitalized keys (tests provide "Rainfall"/"TrackTemp")
        rainfall = weather_dict.get("rainfall")
        if rainfall is None:
            rainfall = weather_dict.get("Rainfall", False)
        if rainfall:
            return "rain"

        track_temp = weather_dict.get("tracktemp")
        if track_temp is None:
            track_temp = weather_dict.get("TrackTemp")

        if track_temp is not None:
            if track_temp >= 40:
                return "hot"
            if track_temp <= 30:
                return "cool"

        return "dry"
    
    # Get tire degradiction rate based on weather
    def get_degradation_multiplier(self, weather_dict: dict) -> float:
        condition = self.get_weather_condition(weather_dict)
        return self.WEATHER_IMPACTS.get(condition, 1.0)
    
    # Check if safety car is active
    def is_safety_car_active(self) -> bool:
        return self.track_status in ["SAFETY_CAR", "VSC Deployed"]
    
    # Get pace advantage when comparing different tire compounds
    # Returns pace advantage (in seconds) of compound_a over compound_b.
    # #Positive = compound_a is faster
    # Negative = compound_a is slower
    def get_compound_advantage(self, compound_a: str, compound_b: str) -> float:
        if not compound_a or not compound_b:
            return 0.0
        
        key = (compound_a.upper(), compound_b.upper())
        return self.COMPOUND_ADVANTAGE.get(key, 0.0)
    
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
        decoded = self.TRACK_STATUS_CODES.get(str(track_status), "GREEN") if track_status is not None else state["track_status"]
        state["track_status"] = decoded
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
        degradation_rate = self.DEGRADATION_RATES.get((state.get("compound") or "").upper(), 0.05)
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
        if driver_ahead not in self.driver_state or driver_behind not in self.driver_state: 
            return None
            
        ahead = self.driver_state[driver_ahead]
        behind = self.driver_state[driver_behind]
        
        # POSITION VALIDATION: Ensure ahead is actually ahead
        ahead_pos = ahead.get("position")
        behind_pos = behind.get("position")
        
        if ahead_pos is not None and behind_pos is not None:
            if ahead_pos >= behind_pos:
                return {
                    "viable": False,
                    "time_delta": 0.0,
                    "confidence": 0.0,
                    "pit_loss": 0.0,
                    "reason": f"Position error: {driver_ahead} (P{ahead_pos}) is not ahead of {driver_behind} (P{behind_pos})"
                }
        
        # Checking that we have necessary info 
        if not ahead["lap_times"] or not behind["lap_times"]:
            return {
                "viable": False,
                "time_delta": 0.0,
                "confidence": 0.0,
                "pit_loss": 0.0,
                "reason": "Insufficient lap time data"
            }
            
        # Calculating the pit loss from track conditions
        pit_loss = self.SAFETY_CAR_PIT_LOSS if self.is_safety_car_active() else self.PIT_LOSS
        
        # Getting the degradation rates
        ahead_degradation = self.DEGRADATION_RATES.get((ahead.get("compound") or "").upper(), 0.05)
        behind_degradation = self.DEGRADATION_RATES.get((behind.get("compound") or "").upper(), 0.05)
        
        # Apply weather multipliers
        ahead_weather_mult = self.get_degradation_multiplier(ahead["weather"])
        behind_weather_mult = self.get_degradation_multiplier(behind["weather"])
        
        ahead_degradation *= ahead_weather_mult
        behind_degradation *= behind_weather_mult
        
        # Project ahead's next lap (with degradation)
        ahead_projected = ahead["current_pace"] + (ahead["tyre_age"] * ahead_degradation)
        
        # Project behind's lap after pit stop
        behind_best = min(behind["lap_times"][-5:])
        pit_loss_per_lap = pit_loss / self.AMORTIZATION_LAPS
        
        # Account for compound differences
        ahead_compound = ahead.get("compound", "MEDIUM")
        behind_compound = behind.get("compound", "MEDIUM")
        
        # Calculate compound advantage (if behind switches to faster compound)
        compound_advantage = self.get_compound_advantage(behind_compound, ahead_compound)
        
        behind_projected = (behind_best + 
                           pit_loss_per_lap - 
                           self.FRESH_TIRE_ADVANTAGE - 
                           compound_advantage)
        
        # Calculate time delta
        time_delta = ahead_projected - behind_projected
        
        # Viability check
        if self.is_safety_car_active():
            viable_theshold = self.SAFETY_CAR_THRESHOLD
            viable = time_delta > viable_theshold
        else:
            # Normal conditions
            viable = time_delta > 0.0
            
        confidence = min(len(behind["lap_times"]) / 5.0, 1.0)
        
        # Reasoning
        reason = ""
        if self.is_safety_car_active():
            reason = "Safety car active. "
        elif ahead["weather"].get("rainfall"):
            reason = "Rain conditions affecting pace. "
        elif ahead["weather"].get("track_temp", 0) > 40:
            reason = "High track temperature."
        else:
            reason = "Normal racing conditions."
            
        return {
            "viable": viable,
            "time_delta": round(time_delta, 2),
            "confidence": round(confidence, 2),
            "pit_loss": round(pit_loss, 1),
            "ahead_projected": round(ahead_projected, 2),
            "behind_projected": round(behind_projected, 2),
            "ahead_degradation": round(ahead_degradation, 4),
            "ahead_tire_age": ahead["tyre_age"],
            "ahead_compound": ahead_compound,
            "behind_compound": behind_compound,
            "compound_advantage": round(compound_advantage, 2),
            "weather_condition": self.get_weather_condition(ahead["weather"]),
            "track_status": self.track_status,
            "recommendation": "BOX NOW" if viable else "STAY OUT",
            "reason": reason
        }
        
    # Recommendation during safety car period.
    # Pit if gap to leader is within threshold 
    def get_safety_car_recommendation(self, driver_behind: str, gap_to_leader: float) -> Optional[dict]:
        if driver_behind not in self.driver_state:
            return None
        
        behind = self.driver_state[driver_behind]
        
        # During safety car, pit if gap to leader is within threshold
        if self.is_safety_car_active() and gap_to_leader <= self.SAFETY_CAR_THRESHOLD:
            return {
                "viable": True,
                "recommendation": "BOX NOW (Safety Car window)",
                "reason": f"Safety car gap threshold ({gap_to_leader:.2f}s < {self.SAFETY_CAR_THRESHOLD}s)",
                "pit_loss": self.SAFETY_CAR_PIT_LOSS
            }
            
        return None
    
    # Return a list of all drivers
    def get_all_drivers(self)-> List[str]:
        return list(self.driver_state.keys())
    
    # Get driver state
    def get_driver_state(self, driver: str) -> Optional[dict]:
        return self.driver_state.get(driver, None)
    
    # Clear all driver state
    def reset(self):
        self.driver_state.clear()
        self.weather.clear()
        self.track_status = "GREEN"