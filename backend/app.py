import json
import logging
import threading
import time
import redis
from datetime import datetime
from flask import Flask, jsonify, request  
from flask_socketio import SocketIO, emit, disconnect
from flask_cors import CORS
from kafka import KafkaConsumer
from strategy_engine import UndercutEngine

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)
app.config['SECRET_KEY'] = 'f1-undercut-secret-key-2024'

# Enable CORS for all routes
CORS(app, origins=["http://localhost:3000", "http://localhost:5173"])

# Intitialize SocketIO
socketio = SocketIO(app, cors_allowed_origins=["http://localhost:3000", "http://localhost:5173"])

# Global Variables
engine = UndercutEngine()
kafka_consumer = None 
connected_clients = set() # Track connected clients
app_start_time = datetime.now()
messages_processed = 0
undercuts_detected = 0
current_race_lap = 0
last_race_update = None
last_checked_lap = 0

# Initialize Redis client (optional - graceful degradation if unavailable)
try:
    redis_client = redis.Redis(
        host='localhost',
        port=6379,
        db=0,
        decode_responses=True
    )
    redis_client.ping()
    logger.info("Connected to Redis")
except Exception as e:
    logger.warning(f"Redis unavailable: {e}. Continuing without caching.")
    redis_client = None

# Background Kafka Listener 
# Background thread that consumes Kafka messages and broadcasts via Websocket 
def kafka_listener():
    global kafka_consumer, messages_processed, current_race_lap, last_race_update, undercuts_detected, last_checked_lap

    try:
        kafka_consumer = KafkaConsumer(
            'f1-telemetry',
            bootstrap_servers='localhost:9092',
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='f1-flask-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        logger.info("Kafka consumer started in background thread.")
        
        check_interval = 5
        
        for message in kafka_consumer:
            data = message.value
            messages_processed += 1
            
            # Extract key info 
            driver = data.get('Driver')
            lap_num = data.get('LapNumber')
            lap_time = data.get('LapTime')
            compound = data.get('Compound')
            tyre_life = data.get('TyreLife')
            position = data.get('Position')
            session_name = data.get('SessionName', {}).get('EventName')
            
            # Set track on first message with session name 
            if session_name and not engine.track_name:
                engine.set_track(session_name)
                logger.info(f"Track set to: {engine.track_name}")
                # Persist race session to Redis
                if redis_client:
                    try:
                        redis_client.set('f1:race:session', session_name)
                        redis_client.expire('f1:race:session', 3600)  # 1 hour expiry
                        logger.info(f"Race session persisted to Redis: {session_name}")
                    except Exception as e:
                        logger.error(f"Failed to persist race session: {e}")
                socketio.emit('track_info', {
                    'track': session_name,
                    'config': engine.get_track_config()
                }, broadcast=True)
                
            # Update engine with lap data
            engine.update_driver_state(driver, data)
            cache_driver_state(driver)
            current_race_lap = lap_num if lap_num else current_race_lap
            
            # Broadcast lap update to all clients
            driver_state = engine.get_driver_state(driver)
            race_update = {
                'timestamp': datetime.now().isoformat(),
                'driver': driver,
                'lap_number': lap_num,
                'lap_time': lap_time,
                'compound': compound,
                'tyre_life': tyre_life,
                'position': position,
                'current_pace': driver_state.get('current_pace', 0) if driver_state else 0,
                'weather': data.get('Weather', {}),
                'track_status': data.get('TrackStatus', 'GREEN')
            }
            
            last_race_update = race_update
            socketio.emit('race_update', race_update, broadcast=True)
            
            logger.info(f"[MSG {messages_processed}] Lap {lap_num} | {driver} | {compound} | Pos {position}")
            
             # Track which drivers have data this lap (for DNF detection)
            if not hasattr(kafka_listener, 'active_drivers_this_lap'):
                kafka_listener.active_drivers_this_lap = set()
            kafka_listener.active_drivers_this_lap.add(driver)
            
            # Check for undercut opportunities every N race laps
            if (current_race_lap > 0 and 
                current_race_lap % check_interval == 0 and 
                current_race_lap != last_checked_lap):
                
                # Mark drivers who haven't appeared recently as retired
                engine.mark_inactive_drivers_retired(kafka_listener.active_drivers_this_lap)
                kafka_listener.active_drivers_this_lap = set()  # Reset for next lap
                logger.info(f"Checking undercuts at Race Lap {current_race_lap}")
                undercut_alerts = check_and_broadcast_undercuts()
                undercuts_detected += len(undercut_alerts)
                last_checked_lap = current_race_lap
                
    except Exception as e:
        logger.error(f"Kafka listener error: {e}", exc_info=True)
    finally:
        if kafka_consumer:
            kafka_consumer.close()
        logger.info("Kafka consumer closed.")
        
def cache_driver_state(driver: str):
    """Cache driver state in Redis."""
    if not redis_client:
        return
    
    try:
        state = engine.get_driver_state(driver)
        if state:
            cache_state = {
                "lap_number": str(state['lap_number']),
                "tyre_age": str(state['tyre_age']),
                "compound": state['compound'] or "UNKNOWN",
                "position": str(state['position']) if state['position'] else "0",
                "current_pace": str(state['current_pace']),
                "stint": str(state.get('stint', 0))
            }
            redis_client.hset(f"f1:driver:{driver}", mapping=cache_state)
            redis_client.expire(f"f1:driver:{driver}", 3600)
    except Exception as e:
        logger.error(f"Failed to cache driver state: {e}")

def cache_prediction(ahead: str, behind: str, result: dict):
    """Cache prediction results in Redis."""
    if not redis_client:
        return
    
    try:
        cache_key = f"f1:prediction:{ahead}:{behind}:{current_race_lap}"
        redis_client.hset(cache_key, mapping={
            "timestamp": datetime.now().isoformat(),
            "viable": str(result['viable']),
            "time_delta": str(result['time_delta']),
            "confidence": str(result['confidence']),
            "recommendation": result['recommendation']
        })
        redis_client.expire(cache_key, 3600)
        # Add to sorted set for quick retrieval of recent predictions
        redis_client.zadd('f1:predictions:recent', {f'{ahead}:{behind}': current_race_lap})
    except Exception as e:
        logger.error(f"Failed to cache prediction: {e}")

# Check for undercut opportunities and broadcast alerts
def check_and_broadcast_undercuts():
    sorted_drivers = get_sorted_drivers_by_position()
    alerts = []
    
    if len(sorted_drivers) < 2:
        return alerts
    
    for i in range(len(sorted_drivers) - 1):
        ahead = sorted_drivers[i]
        behind = sorted_drivers[i + 1]
        
        result = engine.predict_undercut_window(ahead, behind) 
        
        if result and result['viable']:
            # Calculate actual gap between drivers
            gap = engine.calculate_gap_ahead(ahead, behind)
            
            alert = {
                'timestamp': datetime.now().isoformat(),
                'ahead': ahead,
                'behind': behind,
                'current_gap': result.get('current_gap', gap),
                'laps_to_overcome': result.get('laps_to_overcome', 'N/A'),
                'time_delta': result['time_delta'],
                'confidence': result['confidence'],
                'recommendation': result['recommendation'],
                'pit_loss': result['pit_loss'],
                'ahead_projected': result['ahead_projected'],
                'behind_projected': result['behind_projected'],
                'ahead_compound': result.get('ahead_compound', 'UNKNOWN'),
                'behind_compound': result.get('behind_compound', 'UNKNOWN'),
                'compound_advantage': result.get('compound_advantage', 0.0),
                'weather_condition': result.get('weather_condition', 'dry'),
                'track_status': result['track_status'],
                'reason': result.get('reason', '')
            }
            alerts.append(alert)
            cache_prediction(ahead, behind, result)
            socketio.emit('undercut_alert', alert, broadcast=True)
            logger.info(f"UNDERCUT ALERT: {ahead} vs {behind} | Delta: {result['time_delta']}s")
    
    return alerts
        
# Return drivers sorted by their current position
def get_sorted_drivers_by_position():
    """Return drivers sorted by position, filtering out retired/DNF drivers."""
    drivers = engine.get_all_drivers()
    driver_positions = []
    
    for driver in drivers:
        state = engine.get_driver_state(driver)
        if state:
            position = state.get('position')
            
            # Handle None positions (retired/DNF drivers)
            if position is None:
                position = 999
            
            # Skip retired drivers
            if state.get('retired', False):
                continue
                
            driver_positions.append((driver, position))
    
    # Filter out invalid positions and sort
    valid_positions = [(d, p) for d, p in driver_positions if p < 900]
    valid_positions.sort(key=lambda x: x[1])
    
    return [driver for driver, _ in valid_positions]

# Flask Routes
@app.route('/api/status', methods=['GET'])
# Health check endpoint
def health_check():
    uptime = datetime.now() - app_start_time

    return jsonify({
        'status': 'ok',
        'timestamp': datetime.now().isoformat(),
        'uptime_seconds': uptime.total_seconds(),
        'messages_processed': messages_processed,
        'current_lap': current_race_lap,
        'undercuts_detected': undercuts_detected,
        'connected_clients': len(connected_clients),
        'drivers_tracked': len(engine.get_all_drivers()),
        'drivers': engine.get_all_drivers(),
        'track': engine.track_name or 'Not set',
        'last_update': last_race_update
    }), 200
    
@app.route('/api/drivers', methods=['GET'])
# Get all tracked drivers and their current state 
def get_drivers():
    drivers = engine.get_all_drivers()
    driver_data = []
    
    for driver in sorted(drivers):
        state = engine.get_driver_state(driver)
        if state:
            driver_data.append({
                'name': driver,
                'lap_number': state['lap_number'],
                'position': state['position'],
                'compound': state['compound'],
                'tyre_age': state['tyre_age'],
                'current_pace': round(state['current_pace'], 2) if state['current_pace'] else 0.0,
                'stint': state.get('stint', 0),
                'retired': state.get('retired', False),
                'track_status': state.get('track_status', 'GREEN')
            })
    
    return jsonify(driver_data), 200

@app.route('/api/track_config', methods=['GET'])
# Get current track configuration
def get_track_config():
    return jsonify(engine.get_track_config()), 200

@app.route('/api/driver/<driver_name>', methods=['GET'])
def get_specific_driver(driver_name):
    """Get detailed state for a specific driver."""
    state = engine.get_driver_state(driver_name)
    if state:
        return jsonify({
            'driver': driver_name,
            'lap_number': state['lap_number'],
            'position': state['position'],
            'compound': state['compound'],
            'tyre_age': state['tyre_age'],
            'current_pace': round(state['current_pace'], 2),
            'stint': state.get('stint', 0),
            'retired': state.get('retired', False),
            'cumulative_time': state.get('cumulative_time', 0),
            'weather': state.get('weather', {}),
            'track_status': state.get('track_status', 'GREEN')
        }), 200
    else:
        return jsonify({'error': 'Driver not found'}), 404

@app.route('/api/projected-pace/<driver_name>', methods=['GET'])
def get_projected_pace(driver_name):
    """Get projected pace for a driver over next N laps."""
    future_laps = request.args.get('laps', default=5, type=int)
    projected = engine.calculate_projected_pace(driver_name, future_laps)
    
    if projected:
        return jsonify({
            'driver': driver_name,
            'future_laps': future_laps,
            'projected_pace': round(projected, 2)
        }), 200
    else:
        return jsonify({'error': 'Cannot calculate projection'}), 400

@app.route('/api/gap/<driver_ahead>/<driver_behind>', methods=['GET'])
def get_gap_between_drivers(driver_ahead, driver_behind):
    """Calculate time gap between two drivers."""
    gap = engine.calculate_gap_ahead(driver_ahead, driver_behind)
    return jsonify({
        'driver_ahead': driver_ahead,
        'driver_behind': driver_behind,
        'gap_seconds': round(gap, 2)
    }), 200

@app.route('/api/compound-advantage', methods=['GET'])
def get_compound_advantage():
    """Get pace advantage of one tire compound vs another."""
    compound_a = request.args.get('compound_a', type=str)
    compound_b = request.args.get('compound_b', type=str)
    
    if not compound_a or not compound_b:
        return jsonify({'error': 'Missing compound_a or compound_b parameters'}), 400
    
    advantage = engine.get_compound_advantage(compound_a, compound_b)
    return jsonify({
        'compound_a': compound_a,
        'compound_b': compound_b,
        'advantage_seconds_per_lap': round(advantage, 2),
        'description': f"{compound_a} is {abs(advantage):.2f}s/lap {'faster' if advantage > 0 else 'slower'} than {compound_b}"
    }), 200

@app.route('/api/safety-car-status', methods=['GET'])
def get_safety_car_status():
    """Check if safety car is currently active."""
    return jsonify({
        'active': engine.is_safety_car_active(),
        'track_status': engine.track_status
    }), 200

@app.route('/api/weather/<driver_name>', methods=['GET'])
def get_driver_weather(driver_name):
    """Get current weather conditions for a driver's location."""
    state = engine.get_driver_state(driver_name)
    if state and state.get('weather'):
        weather_dict = state['weather']
        condition = engine.get_weather_condition(weather_dict)
        return jsonify({
            'driver': driver_name,
            'condition': condition,
            'weather_data': weather_dict
        }), 200
    else:
        return jsonify({'error': 'Weather data not available'}), 404

@app.route('/api/reset', methods=['POST'])
def reset_engine():
    """Reset the strategy engine (clear all state for new race)."""
    engine.reset()
    global messages_processed, undercuts_detected, current_race_lap, last_checked_lap
    messages_processed = 0
    undercuts_detected = 0
    current_race_lap = 0
    last_checked_lap = 0
    
    logger.info("Engine and counters reset")
    socketio.emit('engine_reset', {'timestamp': datetime.now().isoformat()}, broadcast=True)
    
    return jsonify({
        'status': 'reset',
        'timestamp': datetime.now().isoformat()
    }), 200

@app.route('/api/driver/<driver_name>', methods=['GET'])
def get_specific_driver(driver_name):
    """Get specific driver's detailed state."""
    state = engine.get_driver_state(driver_name)
    if state:
        return jsonify({
            'driver': driver_name,
            'lap_number': state['lap_number'],
            'position': state['position'],
            'compound': state['compound'],
            'tyre_age': state['tyre_age'],
            'current_pace': round(state['current_pace'], 2) if state['current_pace'] else 0.0,
            'stint': state.get('stint', 0),
            'retired': state.get('retired', False),
            'cumulative_time': round(state.get('cumulative_time', 0), 2),
            'weather': state.get('weather', {}),
            'track_status': state.get('track_status', 'GREEN'),
            'lap_times': [round(lt, 2) for lt in state.get('lap_times', [])[-5:]]
        }), 200
    else:
        return jsonify({'error': f'Driver {driver_name} not found'}), 404

@app.route('/api/projected-pace/<driver_name>', methods=['GET'])
def get_projected_pace(driver_name):
    """Get projected pace for a driver over next N laps."""
    future_laps = request.args.get('laps', default=5, type=int)
    
    if future_laps < 1 or future_laps > 20:
        return jsonify({'error': 'Invalid lap count (must be 1-20)'}), 400
    
    projected = engine.calculate_projected_pace(driver_name, future_laps)
    
    if projected and projected > 0:
        state = engine.get_driver_state(driver_name)
        return jsonify({
            'driver': driver_name,
            'future_laps': future_laps,
            'projected_pace': round(projected, 2),
            'current_pace': round(state['current_pace'], 2) if state and state['current_pace'] else 0.0,
            'tyre_age': state['tyre_age'] if state else 0,
            'compound': state['compound'] if state else 'UNKNOWN'
        }), 200
    else:
        return jsonify({'error': 'Cannot calculate projection for this driver'}), 400

@app.route('/api/gap', methods=['GET'])
def get_gap_between_drivers():
    """Calculate gap between two drivers."""
    driver_ahead = request.args.get('ahead', type=str)
    driver_behind = request.args.get('behind', type=str)
    
    if not driver_ahead or not driver_behind:
        return jsonify({'error': 'Missing driver parameters (ahead, behind)'}), 400
    
    gap = engine.calculate_gap_ahead(driver_ahead, driver_behind)
    
    return jsonify({
        'driver_ahead': driver_ahead,
        'driver_behind': driver_behind,
        'gap_seconds': round(gap, 2)
    }), 200

@app.route('/api/compound-advantage', methods=['GET'])
def get_compound_advantage():
    """Get pace advantage of one compound vs another."""
    compound_a = request.args.get('compound_a', type=str)
    compound_b = request.args.get('compound_b', type=str)
    
    if not compound_a or not compound_b:
        return jsonify({'error': 'Missing compound parameters (compound_a, compound_b)'}), 400
    
    advantage = engine.get_compound_advantage(compound_a, compound_b)
    
    return jsonify({
        'compound_a': compound_a.upper(),
        'compound_b': compound_b.upper(),
        'advantage_seconds_per_lap': round(advantage, 2),
        'description': f"{compound_a.upper()} is {abs(advantage):.2f}s/lap {'faster' if advantage > 0 else 'slower'} than {compound_b.upper()}"
    }), 200

@app.route('/api/safety-car-status', methods=['GET'])
def get_safety_car_status():
    """Check if safety car is active."""
    return jsonify({
        'active': engine.is_safety_car_active(),
        'track_status': engine.track_status
    }), 200

@app.route('/api/reset', methods=['POST'])
def reset_engine():
    """Reset the strategy engine (clear all state)."""
    engine.reset()
    global messages_processed, undercuts_detected, current_race_lap, last_checked_lap
    messages_processed = 0
    undercuts_detected = 0
    current_race_lap = 0
    last_checked_lap = 0
    
    logger.info("Engine and counters reset")
    socketio.emit('engine_reset', {'timestamp': datetime.now().isoformat()}, broadcast=True)
    
    return jsonify({
        'status': 'reset',
        'timestamp': datetime.now().isoformat(),
        'message': 'Strategy engine and counters have been reset'
    }), 200

# SocketIO Handlers
# Handle client connection
@socketio.on('connect')
def handle_connect():
    client_id = request.sid
    connected_clients.add(client_id)
    logger.info(f"Client connected: {client_id} (Total: {len(connected_clients)})")
    
    # Send current status to newly connected client
    emit('connection_response', {
        'status': 'connected',
        'client_id': client_id,
        'timestamp': datetime.now().isoformat(),
        'track': engine.track_name,
        'messages_processed': messages_processed,
        'drivers': engine.get_all_drivers()
    })
    
    # Broadcast updated client count to all clients
    socketio.emit('client_count', {'count': len(connected_clients)}, broadcast=True)
    
@socketio.on('disconnect')
# Handle client disconnection
def handle_disconnect():
    client_id = request.sid
    connected_clients.discard(client_id)
    logger.info(f"Client disconnected: {client_id} (Total: {len(connected_clients)})")
    
    # Broadcast updated client count to all clients
    socketio.emit('client_count', {'count': len(connected_clients)}, broadcast=True)
    
@socketio.on('request_status')
# Handle client request for current status
def handle_status_request():
    uptime = datetime.now() - app_start_time
    
    emit('status_response', {
        'timestamp': datetime.now().isoformat(),
        'uptime_seconds': uptime.total_seconds(),
        'messages_processed': messages_processed,
        'current_lap': current_race_lap,
        'undercuts_detected': undercuts_detected,
        'connected_clients': len(connected_clients),
        'drivers_tracked': len(engine.get_all_drivers()),
        'track': engine.track_name
    })
    
@socketio.on('request_drivers')
# Handle client request for driver data
def handle_drivers_request():
    drivers = engine.get_all_drivers()
    driver_data = []
    
    for driver in sorted(drivers):
        state = engine.get_driver_state(driver)
        if state:
            driver_data.append({
                'name': driver,
                'lap_number': state['lap_number'],
                'position': state['position'],
                'compound': state['compound'],
                'tyre_age': state['tyre_age'],
                'current_pace': round(state['current_pace'], 2) if state['current_pace'] else 0.0,
                'stint': state.get('stint', 0),
                'retired': state.get('retired', False)
            })
    
    emit('drivers_response', driver_data)
    
# Main Method
if __name__ == '__main__':
    logger.info("Starting F1 Undercut Engine Flask Backend...")
    logger.info("CORS enabled for: http://localhost:3000, http://localhost:5173")
    
    # Start Kafka listener in background thread
    listener_thread = threading.Thread(target=kafka_listener, daemon=True)
    listener_thread.start()
    logger.info("Kafka listener thread started")
    
    # Run Flask app with SocketIO
    try:
        socketio.run(
            app,
            host='0.0.0.0',
            port=5000,
            debug=False,
            allow_unsafe_werkzeug=True
        )
    except KeyboardInterrupt:
        logger.info("Flask app stopped by user")
    finally:
        logger.info("Backend shutdown complete")