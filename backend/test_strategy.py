import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from strategy_engine import UndercutEngine
from datetime import timedelta

# Test cases to check lap_time_to_seconds funtion
def test_lap_time_conversion():
    """Test lap time string parsing."""
    engine = UndercutEngine()
    
    assert engine.lap_time_to_seconds(timedelta(minutes=1, seconds=15.5)) == 75.5
    assert engine.lap_time_to_seconds("0:01:15.500") == 75.5
    assert engine.lap_time_to_seconds("1:15.500") == 75.5
    
    print("Lap time conversion tests passed")

# Test cases to parse_weather_data function
def test_weather_parsing():
    engine = UndercutEngine()
    
    weather_data = {
        "AirTemp": 23.0,
        "Humidity": 51.9,
        "Pressure": 992.4,
        "Rainfall": False,
        "TrackTemp": 37.8,
        "WindDirection": 166,
        "WindSpeed": 0.8
    }
    
    parsed = engine.parse_weather_data(weather_data)
    assert parsed["airtemp"] == 23.0
    assert parsed["tracktemp"] == 37.8
    assert parsed["rainfall"] == False
    
    print("Weather parsing tests passed")

# Test cases to check get_weather_condition function
def test_weather_condition_detection():
    engine = UndercutEngine()
    
    # Dry conditions
    dry_weather = {"TrackTemp": 35.0, "Rainfall": False}
    assert engine.get_weather_condition(dry_weather) == "dry"
    
    # Rainy conditions
    rainy_weather = {"TrackTemp": 25.0, "Rainfall": True}
    assert engine.get_weather_condition(rainy_weather) == "rain"
    
    # Hot conditions
    hot_weather = {"TrackTemp": 42.0, "Rainfall": False}
    assert engine.get_weather_condition(hot_weather) == "hot"
    
    # Cool conditions
    cool_weather = {"TrackTemp": 28.0, "Rainfall": False}
    assert engine.get_weather_condition(cool_weather) == "cool"
    
    print("Weather condition detection tests passed")

# Test cases to check get_degradation_multiplier function
def test_weather_degradation_multiplier():
    engine = UndercutEngine()
    
    # Dry (baseline 1.0)
    dry = {"TrackTemp": 35.0, "Rainfall": False}
    assert engine.get_degradation_multiplier(dry) == 1.0
    
    # Hot (increased degradation)
    hot = {"TrackTemp": 42.0, "Rainfall": False}
    assert engine.get_degradation_multiplier(hot) == 1.2
    
    # Cool (reduced degradation)
    cool = {"TrackTemp": 28.0, "Rainfall": False}
    assert engine.get_degradation_multiplier(cool) == 0.9
    
    # Rain (reduced degradation)
    rain = {"TrackTemp": 20.0, "Rainfall": True}
    assert engine.get_degradation_multiplier(rain) == 0.5
    
    print("Weather degradation multiplier tests passed")

# Test cases to check update_track_status function 
def test_track_status():
    engine = UndercutEngine()
    
    # Initially green
    assert not engine.is_safety_car_active()
    
    # Update to safety car
    engine.update_track_status("4")
    assert engine.track_status == "SAFETY_CAR"
    assert engine.is_safety_car_active()
    
    # Update to VSC
    engine.update_track_status("6")
    assert engine.track_status == "VSC Deployed"
    assert engine.is_safety_car_active()
    
    # Back to green
    engine.update_track_status("1")
    assert engine.track_status == "GREEN"
    assert not engine.is_safety_car_active()
    
    print("Track status tests passed")

# Test cases to check update_driver_state function with weather data
def test_update_driver_state_with_weather():
    engine = UndercutEngine()
    
    weather = {
        "AirTemp": 23.0,
        "Humidity": 51.9,
        "Pressure": 992.4,
        "Rainfall": False,
        "TrackTemp": 37.8,
        "WindDirection": 166,
        "WindSpeed": 0.8
    }
    
    for lap_num in range(1, 6):
        engine.update_driver_state("HAM", {
            "LapNumber": lap_num,
            "LapTime": 75.5,
            "Compound": "SOFT",
            "TyreLife": lap_num,
            "Position": 1,
            "Weather": weather,
            "TRACK_STATUS": "1"
        })
    
    state = engine.driver_state["HAM"]
    assert state["weather"]["tracktemp"] == 37.8
    assert state["track_status"] == "GREEN"
    assert len(state["lap_times"]) == 5
    
    print("Driver state with weather tests passed")

def test_undercut_with_hot_weather():
    """Test undercut viability with hot track (increased degradation)."""
    engine = UndercutEngine()
    
    hot_weather = {"TrackTemp": 42.0, "Rainfall": False}
    normal_weather = {"TrackTemp": 35.0, "Rainfall": False}
    
    # Driver A (leader, old SOFT, hot track)
    for lap_num in range(1, 31):
        engine.update_driver_state("VER", {
            "LapNumber": lap_num,
            "LapTime": 75.5,
            "Compound": "SOFT",
            "TyreLife": lap_num,
            "Position": 1,
            "Weather": hot_weather,
            "TRACK_STATUS": "1"
        })
    
    # Driver B (2nd, fresh SOFT, normal weather)
    for lap_num in range(1, 21):
        engine.update_driver_state("HAM", {
            "LapNumber": lap_num,
            "LapTime": 74.0,
            "Compound": "SOFT",
            "TyreLife": lap_num,
            "Position": 2,
            "Weather": normal_weather,
            "TRACK_STATUS": "1"
        })
    
    result = engine.predict_undercut_window("VER", "HAM")
    
    assert result is not None
    assert result["weather_condition"] == "hot"
    # Hot track = more degradation for leader = more likely undercut
    
    print("Undercut with hot weather tests passed")

# Test cases to check undercut viability in rain
def test_undercut_with_rain():
    engine = UndercutEngine()
    
    rain_weather = {"TrackTemp": 20.0, "Rainfall": True}
    
    # Both drivers in rain
    for lap_num in range(1, 21):
        engine.update_driver_state("VER", {
            "LapNumber": lap_num,
            "LapTime": 76.5,
            "Compound": "SOFT",
            "TyreLife": lap_num,
            "Position": 1,
            "Weather": rain_weather,
            "TRACK_STATUS": "1"
        })
    
    for lap_num in range(1, 21):
        engine.update_driver_state("HAM", {
            "LapNumber": lap_num,
            "LapTime": 77.0,
            "Compound": "SOFT",
            "TyreLife": lap_num,
            "Position": 2,
            "Weather": rain_weather,
            "TRACK_STATUS": "1"
        })
    
    result = engine.predict_undercut_window("VER", "HAM")
    
    assert result is not None
    assert result["weather_condition"] == "rain"
    print(f"Rain condition result: {result}")
    
    print("Undercut in rain tests passed")

# Test cases to check undercut viability in normal dry conditions
def test_undercut_normal_conditions():
    engine = UndercutEngine()
    
    normal_weather = {"TrackTemp": 35.0, "Rainfall": False}
    
    for lap_num in range(1, 31):
        engine.update_driver_state("VER", {
            "LapNumber": lap_num,
            "LapTime": 75.5,
            "Compound": "SOFT",
            "TyreLife": lap_num,
            "Position": 1,
            "Weather": normal_weather,
            "TRACK_STATUS": "1"
        })
    
    for lap_num in range(1, 21):
        engine.update_driver_state("HAM", {
            "LapNumber": lap_num,
            "LapTime": 74.0,
            "Compound": "SOFT",
            "TyreLife": lap_num,
            "Position": 2,
            "Weather": normal_weather,
            "TRACK_STATUS": "1"
        })
    
    result = engine.predict_undercut_window("VER", "HAM")
    assert result["viable"] == True
    assert result["pit_loss"] == 20.0  # normal pit loss
    
    print("Undercut normal conditions tests passed")

# Test cases to check undercut viability during safety car
def test_undercut_safety_car():
    engine = UndercutEngine()
    
    normal_weather = {"TrackTemp": 35.0, "Rainfall": False}
    
    # Set safety car conditions
    engine.update_track_status("4")  # Safety car
    
    for lap_num in range(1, 31):
        engine.update_driver_state("VER", {
            "LapNumber": lap_num,
            "LapTime": 75.5,
            "Compound": "SOFT",
            "TyreLife": lap_num,
            "Position": 1,
            "Weather": normal_weather,
            "TRACK_STATUS": "4"
        })
    
    for lap_num in range(1, 21):
        engine.update_driver_state("HAM", {
            "LapNumber": lap_num,
            "LapTime": 74.0,
            "Compound": "SOFT",
            "TyreLife": lap_num,
            "Position": 2,
            "Weather": normal_weather,
            "TRACK_STATUS": "4"
        })
    
    result = engine.predict_undercut_window("VER", "HAM")
    
    assert result is not None
    assert result["pit_loss"] == 10.0  # safety car pit loss
    assert result["track_status"] == "SAFETY_CAR"
    
    print("Undercut safety car tests passed")

# Test cases to check safety car recommendation
def test_safety_car_recommendation():
    engine = UndercutEngine()
    
    normal_weather = {"TrackTemp": 35.0, "Rainfall": False}
    
    # Set safety car
    engine.update_track_status("4")
    
    engine.update_driver_state("HAM", {
        "LapNumber": 10,
        "LapTime": 75.0,
        "Compound": "SOFT",
        "TyreLife": 10,
        "Position": 2,
        "Weather": normal_weather,
        "TRACK_STATUS": "4"
    })
    
    # Small gap to leader
    rec = engine.get_safety_car_recommendation("HAM", gap_to_leader=3.0)
    
    assert rec is not None
    assert rec["viable"] == True
    assert rec["pit_loss"] == 10.0
    
    print("Safety car recommendation tests passed")

# Test position validation
def test_position_validation():
    """Test that undercut only works when positions are correct."""
    engine = UndercutEngine()
    
    normal_weather = {"TrackTemp": 35.0, "Rainfall": False}
    
    # Driver A (position 1)
    for lap_num in range(1, 21):
        engine.update_driver_state("VER", {
            "LapNumber": lap_num,
            "LapTime": 75.5,
            "Compound": "SOFT",
            "TyreLife": lap_num,
            "Position": 1,
            "Weather": normal_weather,
            "TRACK_STATUS": "1"
        })
    
    # Driver B (position 3 - NOT directly behind)
    for lap_num in range(1, 21):
        engine.update_driver_state("HAM", {
            "LapNumber": lap_num,
            "LapTime": 74.0,
            "Compound": "SOFT",
            "TyreLife": lap_num,
            "Position": 3,
            "Weather": normal_weather,
            "TRACK_STATUS": "1"
        })
    
    # Try to predict undercut with HAM ahead of VER (wrong order)
    result = engine.predict_undercut_window("HAM", "VER")
    
    assert result is not None
    assert result["viable"] == False
    assert "Position error" in result["reason"]
    
    print("Position validation tests passed")

# Test tire compound mixing advantage
def test_compound_mixing():
    engine = UndercutEngine()
    
    normal_weather = {"TrackTemp": 35.0, "Rainfall": False}
    
    # Driver A on MEDIUM tires (position 1)
    for lap_num in range(1, 21):
        engine.update_driver_state("VER", {
            "LapNumber": lap_num,
            "LapTime": 76.0,
            "Compound": "MEDIUM",
            "TyreLife": lap_num,
            "Position": 1,
            "Weather": normal_weather,
            "TRACK_STATUS": "1"
        })
    
    # Driver B on SOFT tires (position 2, will pit for fresh SOFT)
    for lap_num in range(1, 21):
        engine.update_driver_state("HAM", {
            "LapNumber": lap_num,
            "LapTime": 75.0,
            "Compound": "SOFT",
            "TyreLife": lap_num,
            "Position": 2,
            "Weather": normal_weather,
            "TRACK_STATUS": "1"
        })
    
    result = engine.predict_undercut_window("VER", "HAM")
    
    assert result is not None
    assert "compound_advantage" in result
    # SOFT is faster than MEDIUM, so advantage should be positive
    assert result["compound_advantage"] == 0.5
    
    print(f"Compound mixing result: {result}")
    print("Tire compound mixing tests passed")

# Test compound advantage calculation
def test_compound_advantage():
    engine = UndercutEngine()
    
    # SOFT vs MEDIUM
    assert engine.get_compound_advantage("SOFT", "MEDIUM") == 0.5
    
    # SOFT vs HARD
    assert engine.get_compound_advantage("SOFT", "HARD") == 0.8
    
    # MEDIUM vs HARD
    assert engine.get_compound_advantage("MEDIUM", "HARD") == 0.3
    
    # Reverse (slower compounds)
    assert engine.get_compound_advantage("MEDIUM", "SOFT") == -0.5
    assert engine.get_compound_advantage("HARD", "SOFT") == -0.8
    
    # Same compound
    assert engine.get_compound_advantage("SOFT", "SOFT") == 0.0
    
    print("Compound advantage calculation tests passed")

if __name__ == "__main__":
    print("Running Strategy Engine tests (with weather, safety car, & compound mixing)...\n")
    test_lap_time_conversion()
    test_weather_parsing()
    test_weather_condition_detection()
    test_weather_degradation_multiplier()
    test_track_status()
    test_update_driver_state_with_weather()
    test_undercut_with_hot_weather()
    test_undercut_with_rain()
    test_undercut_normal_conditions()
    test_undercut_safety_car()
    test_safety_car_recommendation()
    test_position_validation()
    test_compound_mixing()
    test_compound_advantage()
    print("\nAll 14 tests passed!")