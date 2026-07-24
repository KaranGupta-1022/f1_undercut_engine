# Changelog

## 0.1.0

- FastF1-backed telemetry producer (`producer.py`) replaying historical race sessions lap-by-lap into a Kafka `f1-telemetry` topic
- `UndercutEngine` strategy engine: per-driver state tracking, tyre degradation by compound, weather and safety car adjustments, and undercut viability predictions
- Flask + Flask-SocketIO backend consuming Kafka in a background thread, exposing REST endpoints and broadcasting `race_update` / `undercut_alert` events over WebSockets
- Redis caching for live driver state and recent predictions
- React (Vite) dashboard: live standings, per-driver lap-time chart, undercut alert feed, driver detail panel, circuit map
- Dockerized backend and frontend (Nginx-served, reverse-proxying `/api` and `/socket.io`), with separate dev (`docker-compose.yml`) and production (`docker-compose.prod.yml`) compose files
- Deployed and demoed on a GCP e2-medium VM under trial credit; documented FastF1 live-timing CDN blocking cloud IPs and the local-cache-upload workaround
- GitHub Actions CI: backend pytest + ruff, frontend eslint, on every push/PR
