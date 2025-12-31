import fastf1 
import pandas as pd
from pathlib import Path


# Enable caching to speed up data retrieval
project_root = Path(__file__).resolve().parent.parent
cache_dir = project_root / "cache"
cache_dir.mkdir(parents=True, exist_ok=True)
fastf1.Cache.enable_cache(str(cache_dir))

def main():
    # Load a specific F1 session (2024 Monaco Grand Prix)
    print("Loading session: 2024 Monaco Grand Prix, Race Day")
    session = fastf1.get_session(2024, "Monaco", "Race")
    session.load()
    
    # Display session attributes
    print("\nSession attributes (sample):")
    print(f"Session Name: {session.name}")
    print(f"Start Time:", getattr(session, "start_time", "N/A"))
    print(f"Weather Info:", getattr(session, "weather", "N/A"))
    
    # Explore laps data
    laps =session.laps
    print("\nLaps Columns: ")
    print(list(laps.columns))
    
    
    print("\n First 8 rows of laps")
    print(laps.head(8))
    
    # List unique drivers in the session
    drivers = laps['Driver'].unique()
    print("\nDriver in the session:", drivers[:8])
    
    # Analyze data for the first driver
    driver = drivers[0]
    print(f"\nAnalyzing data for driver: {driver}")
    
    # Filter laps for the selected driver
    try:
        driver_laps = laps.pick_drivers(driver)
    except Exception:
        # Fallback if pick_driver fails
        driver_laps = laps[laps['Driver'] == driver]    
        
    # Display relevant columns for the driver's laps
    cols_of_interest = ["LapNumber", "LapTime", "Compound", "Stint", "TyreLife"]
    existing_cols = [col for col in cols_of_interest if col in driver_laps.columns]
    print(f"\nDriver's laps columns of interest: {existing_cols}"
    )
    print(driver_laps[existing_cols].head(8))
    
    # Identify the fastest lap for the driver
    try:
        fastest = driver_laps.pick_fastest()
    except Exception:
        # Fallback if pick_fastest fails
        # Sort by LapTime and get the first row
        fastest = driver_laps.sort_values('LapTime').iloc[0]
    
    print(f"\nSelected lap for telemetry (fastest / sample):")
    print(fastest)
    
    # Get telemetry data for that lap
    try:
        telemetry = fastest.get_telemetry()
    except Exception:
        # Fallback if get_telemetry fails
        if hasattr(fastest, "name"):
            lap_idx = fastest.name 
            telemetry = session.laps.loc[lap_idx].get_telemetry()
        else:
            telemetry = None
        
    if telemetry is None or telemetry.empty:
        print("No telemetry data available for the selected lap.")
        return
    
    # Print telemetry data columns
    print("\nTelemetry data (sample):")
    print(list(telemetry.columns))
    
    
    # Print the first rows of Speed, Throttle, and Brake data
    for col in ['Speed', 'Throttle', 'Brake']:
        if col in telemetry.columns:
            print(f"\nTelemetry {col} data (first 8 rows):")
            print(telemetry[col].head(8))
        else:
            print(f"\nTelemetry does not contain column: {col}")

if __name__ == "__main__":
    main()