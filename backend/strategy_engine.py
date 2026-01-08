from datetime import timedelta
from typing import Dict, Optional, List

# Track-specific configurations
# Note: Keys must match the first word of EventName from FastF1
TRACK_CONFIG = {
    # Europe
    "Monaco Grand Prix": {
        "pit_loss": 22.0,
        "amortization_laps": 3,
        "fresh_tire_advantage": 1.0,
        "degradation_rates": {"SOFT": 0.05, "MEDIUM": 0.03, "HARD": 0.02}
    },
    "Belgian Grand Prix": {
        "pit_loss": 17.0,
        "amortization_laps": 5,
        "fresh_tire_advantage": 2.5,
        "degradation_rates": {"SOFT": 0.14, "MEDIUM": 0.09, "HARD": 0.05}
    },
    "British Grand Prix": {
        "pit_loss": 20.0,
        "amortization_laps": 4,
        "fresh_tire_advantage": 2.0,
        "degradation_rates": {"SOFT": 0.10, "MEDIUM": 0.06, "HARD": 0.035}
    },
    "Spanish Grand Prix": {
        "pit_loss": 19.0,
        "amortization_laps": 4,
        "fresh_tire_advantage": 2.2,
        "degradation_rates": {"SOFT": 0.12, "MEDIUM": 0.08, "HARD": 0.04}
    },
    "Austrian Grand Prix": {
        "pit_loss": 18.0,
        "amortization_laps": 5,
        "fresh_tire_advantage": 2.8,
        "degradation_rates": {"SOFT": 0.16, "MEDIUM": 0.10, "HARD": 0.06}
    },
    "Hungarian Grand Prix": {
        "pit_loss": 21.0,
        "amortization_laps": 3,
        "fresh_tire_advantage": 1.5,
        "degradation_rates": {"SOFT": 0.08, "MEDIUM": 0.05, "HARD": 0.03}
    },
    "Dutch Grand Prix": {
        "pit_loss": 16.0,
        "amortization_laps": 5,
        "fresh_tire_advantage": 3.0,
        "degradation_rates": {"SOFT": 0.18, "MEDIUM": 0.12, "HARD": 0.07}
    },
    "Italian Grand Prix": {
        "pit_loss": 15.0,
        "amortization_laps": 6,
        "fresh_tire_advantage": 3.2,
        "degradation_rates": {"SOFT": 0.20, "MEDIUM": 0.14, "HARD": 0.08}
    },
    "Singapore Grand Prix": {
        "pit_loss": 24.0,
        "amortization_laps": 2,
        "fresh_tire_advantage": 0.8,
        "degradation_rates": {"SOFT": 0.04, "MEDIUM": 0.02, "HARD": 0.01}
    },
    
    # Asia
    "Japanese Grand Prix": {
        "pit_loss": 18.0,
        "amortization_laps": 5,
        "fresh_tire_advantage": 2.6,
        "degradation_rates": {"SOFT": 0.15, "MEDIUM": 0.10, "HARD": 0.05}
    },
    "Chinese Grand Prix": {
        "pit_loss": 17.0,
        "amortization_laps": 5,
        "fresh_tire_advantage": 2.6,
        "degradation_rates": {"SOFT": 0.15, "MEDIUM": 0.10, "HARD": 0.05}
    },
    "Qatar Grand Prix": {
        "pit_loss": 17.0,
        "amortization_laps": 5,
        "fresh_tire_advantage": 2.7,
        "degradation_rates": {"SOFT": 0.16, "MEDIUM": 0.11, "HARD": 0.06}
    },
    "Azerbaijan Grand Prix": {
        "pit_loss": 18.0,
        "amortization_laps": 5,
        "fresh_tire_advantage": 2.5,
        "degradation_rates": {"SOFT": 0.14, "MEDIUM": 0.09, "HARD": 0.05}
    },
    
    # Americas
    "Australian Grand Prix": {
        "pit_loss": 19.0,
        "amortization_laps": 4,
        "fresh_tire_advantage": 2.1,
        "degradation_rates": {"SOFT": 0.11, "MEDIUM": 0.07, "HARD": 0.04}
    },
    "Miami Grand Prix": {
        "pit_loss": 19.0,
        "amortization_laps": 4,
        "fresh_tire_advantage": 2.0,
        "degradation_rates": {"SOFT": 0.10, "MEDIUM": 0.07, "HARD": 0.04}
    },
    "Canadian Grand Prix": {
        "pit_loss": 21.0,
        "amortization_laps": 3,
        "fresh_tire_advantage": 1.6,
        "degradation_rates": {"SOFT": 0.07, "MEDIUM": 0.04, "HARD": 0.025}
    },
    "Mexico City Grand Prix": {
        "pit_loss": 20.0,
        "amortization_laps": 4,
        "fresh_tire_advantage": 2.1,
        "degradation_rates": {"SOFT": 0.11, "MEDIUM": 0.07, "HARD": 0.04}
    },
    "United States Grand Prix": {
        "pit_loss": 19.0,
        "amortization_laps": 4,
        "fresh_tire_advantage": 2.2,
        "degradation_rates": {"SOFT": 0.12, "MEDIUM": 0.08, "HARD": 0.04}
    },
    "São Paulo Grand Prix": {
        "pit_loss": 20.0,
        "amortization_laps": 3,
        "fresh_tire_advantage": 1.8,
        "degradation_rates": {"SOFT": 0.09, "MEDIUM": 0.06, "HARD": 0.035}
    },
    
    # Middle East
    "Saudi Arabian Grand Prix": {
        "pit_loss": 16.0,
        "amortization_laps": 5,
        "fresh_tire_advantage": 2.9,
        "degradation_rates": {"SOFT": 0.17, "MEDIUM": 0.11, "HARD": 0.06}
    },
    
    # Additional circuits
    "Emilia Romagna Grand Prix": {
        "pit_loss": 17.0,
        "amortization_laps": 5,
        "fresh_tire_advantage": 2.4,
        "degradation_rates": {"SOFT": 0.13, "MEDIUM": 0.09, "HARD": 0.05}
    },
    "Bahrain Grand Prix": {
        "pit_loss": 18.0,
        "amortization_laps": 4,
        "fresh_tire_advantage": 2.4,
        "degradation_rates": {"SOFT": 0.13, "MEDIUM": 0.09, "HARD": 0.05}
    },
    "Las Vegas Grand Prix": {
        "pit_loss": 17.0,
        "amortization_laps": 5,
        "fresh_tire_advantage": 2.5,
        "degradation_rates": {"SOFT": 0.14, "MEDIUM": 0.09, "HARD": 0.05}
    },
    "Abu Dhabi Grand Prix": {
        "pit_loss": 18.0,
        "amortization_laps": 4,
        "fresh_tire_advantage": 2.4,
        "degradation_rates": {"SOFT": 0.13, "MEDIUM": 0.09, "HARD": 0.05}
    },
}

class UndercutEngine:
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
    
    def __init__(self, track=None):
        # driver_state[driver_name] = {
        #   "lap_number": int,
        #   "tyre_age": int,
        #   "compound": str,
        #   "position": int,
        #   "lap_times": [__init__, ...],
        #   "current_pace": float,
        #   "weather": {...},  # current weather conditions
        #   "track_status": str,  # current track status
        #   "stint": int,
        #   "gap_to_leader": float
        # }
        self.driver_state = {}
        self.inactive_count = {}  # Track laps since last data received for each driver
        self.weather = {}
        self.track_status = "GREEN"
        self.track_name = None
        
         # Default constants (overridden by set_track)
        self.PIT_LOSS = 20.0
        self.AMORTIZATION_LAPS = 3
        self.FRESH_TIRE_ADVANTAGE = 1.5
        self.DEGRADATION_RATES = {"SOFT": 0.08, "MEDIUM": 0.05, "HARD": 0.03}
        self.SAFETY_CAR_PIT_LOSS = 10.0
    
        if track:
            self.set_track(track)
      
    # Load track-specific configuration and update instance constants     
    def set_track(self, track_name: str):
        if track_name in TRACK_CONFIG:
            config = TRACK_CONFIG[track_name]
            self.PIT_LOSS = config.get("pit_loss", 20.0)
            self.AMORTIZATION_LAPS = config.get("amortization_laps", 3)
            self.FRESH_TIRE_ADVANTAGE = config.get("fresh_tire_advantage", 1.5)
            self.DEGRADATION_RATES = config.get("degradation_rates", {"SOFT": 0.08, "MEDIUM": 0.05, "HARD": 0.03})
            self.track_name = track_name
        else:
            self.track_name = track_name
            
    # Return current track configuration.
    def get_track_config(self) -> dict:
        return {
            "track_name": self.track_name,
            "pit_loss": self.PIT_LOSS,
            "amortization_laps": self.AMORTIZATION_LAPS,
            "fresh_tire_advantage": self.FRESH_TIRE_ADVANTAGE,
            "degradation_rates": self.DEGRADATION_RATES
        }

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
        lap_seconds = self.lap_time_to_seconds(lap_time)
        
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
                "cumulative_time": 0.0,
                "weather": {},
                "track_status": "GREEN",
                "stint": 1,
                "gap_to_leader": 0.0,
                "gap_ahead": 0.0
            }
            
        # Update driver state
        state = self.driver_state[driver]
        state["lap_number"] = lap_number if lap_number else state["lap_number"]
        state["position"] = position if position else state["position"]
        state["compound"] = compound if compound else state["compound"]
        decoded = self.TRACK_STATUS_CODES.get(str(track_status), "GREEN") if track_status is not None else state["track_status"]
        state["track_status"] = decoded
        
        # Detect pit stop: Stint increments = pit occurred
        previous_stint = state.get("stint", 1)
        if stint and stint != previous_stint:
            state["lap_times"] = []  # Clear old lap times after pit
            state["current_pace"] = 0.0
        
        # Update stint after pit stop check
        state["stint"] = stint if stint else state["stint"]
        
        # Reset inactivity counter when data is received for this driver
        self.inactive_count[driver] = 0
                
        # Parse and store weather data
        if weather:
            state["weather"] = self.parse_weather_data(weather)
            
        # Tire age 
        if tyre_life is not None:
            state["tyre_age"] = tyre_life
            
        # Add lap times 
        if lap_seconds and lap_seconds > 0:
            state["lap_times"].append(lap_seconds)
            if len(state["lap_times"]) > 10:
                state["lap_times"] = state["lap_times"][-10:]
            
            # Update cumulative time
            state["cumulative_time"] += lap_seconds
            
            # Calculate current pace as average of last 3 laps
            recent_laps = state["lap_times"][-3:]
            state["current_pace"] = sum(recent_laps) / len(recent_laps)
    
    # Calculate gap between consecutive drivers based on cumulative time
    def calculate_gap_ahead(self, driver_ahead: str, driver_behind: str) -> float:
    
        if driver_ahead not in self.driver_state or driver_behind not in self.driver_state:
            return 0.0
        
        ahead = self.driver_state[driver_ahead]
        behind = self.driver_state[driver_behind]
        
        # If not on same lap, gap is unreliable
        ahead_lap = ahead.get("lap_number", 0)
        behind_lap = behind.get("lap_number", 0)
        
        if ahead_lap != behind_lap:
            # Approximate: 1 lap = ~90 seconds (average F1 lap)
            lap_diff = abs(ahead_lap - behind_lap)
            return lap_diff * 90.0  # Very rough estimate
        
        # Same lap - use cumulative time difference
        gap = behind.get("cumulative_time", 0.0) - ahead.get("cumulative_time", 0.0)
        
        return max(gap, 0.0)  # Gap can't be negative
            
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
        total_pace = 0
        for lap in range(future_laps):
            tire_wear = tire_age + lap
            degradation_penalty = adjusted_degradation * tire_wear
            total_pace += (current_pace + degradation_penalty)  # each lap has full pace + degradation
        return total_pace / future_laps

    
    # Create a standardized error response with all required fields.
    def _create_error_response(self, reason: str, ahead=None, behind=None) -> dict:
        return {
            "viable": False,
            "time_delta": 0.0,
            "current_gap": 0.0,
            "laps_to_overcome": "N/A",
            "confidence": 0.0,
            "pit_loss": self.SAFETY_CAR_PIT_LOSS if self.is_safety_car_active() else self.PIT_LOSS,
            "ahead_projected": 0.0,
            "behind_projected": 0.0,
            "ahead_degradation": 0.0,
            "ahead_tire_age": 0,
            "ahead_compound": "UNKNOWN",
            "behind_compound": "UNKNOWN",
            "compound_advantage": 0.0,
            "weather_condition": "dry",
            "track_status": self.track_status,
            "recommendation": "STAY OUT",
            "reason": reason
        }
    
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
            return self._create_error_response("Driver state not found")
            
        ahead = self.driver_state[driver_ahead]
        behind = self.driver_state[driver_behind]
        
        # POSITION VALIDATION: Ensure ahead is actually ahead
        ahead_pos = ahead.get("position")
        behind_pos = behind.get("position")
        
        if ahead_pos is not None and behind_pos is not None:
            if ahead_pos > behind_pos:
                return self._create_error_response(
                    f"Position error: {driver_ahead} (P{ahead_pos}) is not ahead of {driver_behind} (P{behind_pos})"
                )
                
         # LAP NUMBER VALIDATION: Warn if drivers are on different laps (lapped cars)
        ahead_lap = ahead.get("lap_number", 0)
        behind_lap = behind.get("lap_number", 0)
        lap_difference = abs(ahead_lap - behind_lap)
        
        if lap_difference > 1:
            return self._create_error_response(
                f"Lap difference too large: {driver_ahead} on Lap {ahead_lap}, {driver_behind} on Lap {behind_lap} ({lap_difference} laps apart)"
            )
        
        # Checking that we have necessary info 
        # Need at least 5 laps for stable predictions
        if not ahead["lap_times"] or not behind["lap_times"]:
            return self._create_error_response("Insufficient lap time data")
        
        # Require minimum lap data for stable predictions
        if len(ahead["lap_times"]) < 5 or len(behind["lap_times"]) < 5:
            return self._create_error_response(
                f"Not enough laps completed (need 5+, have {len(ahead['lap_times'])}/{len(behind['lap_times'])})"
            )

            
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
        
        # Degradation should be reasonable (0.1 - 3.0 seconds per lap)
        ahead_degradation = max(0.1, min(ahead_degradation, 3.0))
        behind_degradation = max(0.1, min(behind_degradation, 3.0))

        # Project ahead's next lap (with degradation)
        ahead_projected = ahead["current_pace"] + (ahead_degradation * ahead["tyre_age"])
        
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
        
        # Calculate time delta (per lap advantage)
        time_delta_per_lap = ahead_projected - behind_projected
        
        # Calculate actual gap between drivers
        current_gap = self.calculate_gap_ahead(driver_ahead, driver_behind)
        
        # CRITICAL: Check if gap is sufficient for undercut
        # Behind driver needs to overcome: current_gap + pit_loss
        # Using their per-lap advantage over multiple laps
        
        # Estimate laps needed to close gap after pit stop
        if time_delta_per_lap > 0:
            # Total deficit after pit stop
            total_deficit = current_gap + pit_loss
            
            # Laps needed to overcome deficit
            laps_to_overcome = total_deficit / time_delta_per_lap
        else:
            laps_to_overcome = 999  # Can't overcome (no advantage)
        
        # For undercut to work: need to overcome deficit within reasonable laps
        # F1 races typically have 10-20 laps left for strategy
        max_recovery_laps = 15
        
        # Final time delta considers both per-lap advantage and gap
        time_delta = time_delta_per_lap 
        
        # Viability check - must consider both pace AND gap
        if self.is_safety_car_active():
            # During safety car, gap becomes less important (field bunched up)
            viable_threshold = self.SAFETY_CAR_THRESHOLD
            viable = time_delta_per_lap > viable_threshold and laps_to_overcome < 20
        else:
            # Normal conditions: need positive time delta AND achievable recovery
            viable = (time_delta_per_lap > 0.0 and 
                     laps_to_overcome <= max_recovery_laps and
                     current_gap > 3.0)  # Minimum 3 second gap required
            
            # If gap is very small (<3s), undercut is too risky
            if current_gap < 3.0:
                reason = f"Gap too small ({current_gap:.1f}s) - risk of traffic after pit stop."
                viable = False
            
        confidence = min(len(behind["lap_times"]) / 10.0, 1.0)
        
        # Reject low confidence predictions
        if confidence < 0.7:
            return self._create_error_response(
                f"Confidence too low ({confidence:.0%}) - need more lap data"
            )
        
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
            
        # Reject unrealistic predictions
        if ahead_projected > 200 or behind_projected > 200:
            return self._create_error_response(
                f"Calculation error: Unrealistic lap times (ahead: {ahead_projected:.1f}s, behind: {behind_projected:.1f}s)"
            )
        
        if abs(time_delta_per_lap) > 100:
            return self._create_error_response(
                f"Calculation error: Unrealistic time delta ({time_delta_per_lap:.1f}s/lap)"
            )
            
        return {
            "viable": viable,
            "time_delta": round(time_delta_per_lap, 2),
            "current_gap": round(current_gap, 2),
            "laps_to_overcome": round(laps_to_overcome, 1) if laps_to_overcome < 999 else "N/A",
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
        
    # Return a list of all drivers
    def get_all_drivers(self)-> List[str]:
        return list(self.driver_state.keys())
    
    # Get driver state
    def get_driver_state(self, driver: str) -> Optional[dict]:
        return self.driver_state.get(driver, None)
    
    # Mark drivers as retired after 1 missed lap (immediate detection on DNF).
    def mark_inactive_drivers_retired(self, active_drivers_this_lap: set):
        for driver in list(self.driver_state.keys()):
            if driver not in active_drivers_this_lap:
                # Increment inactivity counter for drivers with no data this lap
                self.inactive_count[driver] = self.inactive_count.get(driver, 0) + 1
                # Mark retired after 1 missed lap (immediate detection)
                if self.inactive_count[driver] >= 1:
                    self.driver_state[driver]["retired"] = True
            else:
                # Reset counter when driver sends data again
                self.inactive_count[driver] = 0
    
    # Clear all driver state
    def reset(self):
        self.driver_state.clear()
        self.weather.clear()
        self.inactive_count.clear()
        self.track_status = "GREEN"