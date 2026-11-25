# Aegis Edge

Edge-to-cloud telemetry and alerting pipeline built with Go, Protocol Buffers (gRPC), PostgreSQL, Grafana, and a lightweight HTTP/REST API.

## Architecture

- **`edge-agent`**: collects system telemetry (CPU usage, free memory, temperature, and `uptime_seconds` from `/proc/uptime`) and sends `DeviceMessage` streams over gRPC, plus handles cloud commands (`SET_INTERVAL`, `PAUSE`, `RESUME`, `FORCE_FLUSH`, `RECONNECT`, `PING`).
- **`api/proto/v1`**: protobuf definitions (`TelemetryData`, `CloudCommand`, etc.) shared between agent and gateway.
- **`cloud-gateway`**: receives gRPC streams on `:50051`, stores telemetry and alerts in PostgreSQL, exposes `/cmd` for sending commands, and serves REST endpoints on `:8081`.
- **PostgreSQL & Grafana**: stores `telemetry` (including `uptime_seconds`) and `alerts` records for dashboard visualization.

## REST API (`:8081`)

- `GET /api/v1/health` — checks gateway and database connectivity.
- `GET /api/v1/telemetry/latest` — returns the latest telemetry reading (optional `?device_id=...`).
- `GET /api/v1/alerts?limit=20` — returns recent alerts.
- `POST /cmd?action=...` — sends a control command to the connected edge device.

## Local Setup

1. Start PostgreSQL and ensure the `telemetry` table includes `uptime_seconds BIGINT NOT NULL DEFAULT 0`.
2. Start `cloud-gateway` (gRPC `:50051`, HTTP `:8081`).
3. Start `edge-agent` pointing to the gateway.
4. Import the Grafana dashboard JSON and connect it to PostgreSQL.