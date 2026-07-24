# API Reference

Base URL: `http://localhost:5000` (direct backend) or `http://<host>/api` (via the Nginx-fronted production stack, which proxies `/api/*` to the backend).

## REST endpoints

### `GET /`
API index. Returns service name, version, status, and a list of available endpoints.

### `GET /api/status`
Health check.

Response:
```json
{
  "status": "ok",
  "timestamp": "2026-07-24T19:39:45.449993",
  "uptime_seconds": 123.4,
  "messages_processed": 1310,
  "current_lap": 66,
  "undercuts_detected": 3,
  "connected_clients": 1,
  "drivers_tracked": 20,
  "drivers": ["VER", "NOR", "..."],
  "track": "Spanish Grand Prix",
  "last_update": { "...last race_update payload..." }
}
```

### `GET /api/drivers`
List all tracked drivers with their current state (position, compound, tyre age, pace, stint), sorted alphabetically by driver code.

### `GET /api/track-config`
Current track's strategy constants: `pit_loss`, `amortization_laps`, `fresh_tire_advantage`, `degradation_rates`.

### `GET /api/driver/<driver_name>`
Detailed state for one driver: lap number, position, compound, tyre age, current pace, stint, retired flag, cumulative time, weather, track status, and last 5 lap times.

Returns `404` if the driver isn't tracked.

### `GET /api/projected-pace/<driver_name>?laps=5`
Projected average pace over the next N laps (1–20), accounting for tyre degradation. `laps` defaults to 5.

Returns `400` for an out-of-range `laps` value or if a projection can't be calculated.

### `GET /api/gap?ahead=HAM&behind=VER`
Time gap (seconds) between two drivers, based on cumulative race time. Requires both `ahead` and `behind` query params.

### `GET /api/compound-advantage?compound_a=SOFT&compound_b=MEDIUM`
Pace advantage (seconds/lap) of one tyre compound over another, plus a human-readable description. Requires both `compound_a` and `compound_b`.

### `GET /api/safety-car-status`
Whether a safety car / VSC is currently active, and the raw track status string.

### `GET /api/weather/<driver_name>`
Current weather conditions (`condition` + raw weather fields) as last reported for that driver. Returns `404` if not available.

### `POST /api/reset`
Clears all engine state (driver states, counters, current lap) — use to start tracking a fresh session without restarting the backend.

### `GET /api/predictions-log`
Returns the last 50 entries from `predictions.log` (every undercut prediction made, viable or not), parsed as JSON. Returns an empty result with a note if the log doesn't exist yet (producer hasn't run).

## WebSocket events (Socket.IO)

Connect via `socket.io-client` to the same host/port as the REST API (`/socket.io/` path, proxied by Nginx in production).

### Server → client

| Event | Payload | When |
|---|---|---|
| `connection_response` | `{status, client_id, timestamp, track, messages_processed, drivers}` | Sent to a client immediately after it connects |
| `client_count` | `{count}` | Broadcast to all clients on every connect/disconnect |
| `track_info` | `{track, config}` | Sent once, when the session name is first seen in the Kafka stream |
| `race_update` | see `ARCHITECTURE.md` data contracts | Emitted for every processed lap message |
| `undercut_alert` | see `ARCHITECTURE.md` data contracts | Emitted whenever `predict_undercut_window` returns `viable: true` |

### Client → server

| Event | Payload | Response |
|---|---|---|
| `request_status` | none | Server emits `status_response` with the same fields as `/api/status` (minus `drivers`/`track` list, plus `track` name) |
| `request_drivers` | none | Server emits `drivers_response` with the same data as `/api/drivers` |

## Example

```bash
curl http://localhost:5000/api/status
```
