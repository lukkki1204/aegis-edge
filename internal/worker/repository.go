package worker

import (
	"context"
	"database/sql"
	"fmt"

	v1 "github.com/twojanazwa/aegis-edge/api/proto/v1"
)

type Repository struct {
	db *sql.DB
}

func NewRepository(db *sql.DB) *Repository {
	return &Repository{
		db: db,
	}
}

type LatestTelemetry struct {
	ID            int64   `json:"id"`
	DeviceID      string  `json:"device_id"`
	Timestamp     int64   `json:"timestamp"`
	CPUUsage      float64 `json:"cpu_usage"`
	MemoryFree    uint64  `json:"memory_free"`
	MemoryFreeMB  uint64  `json:"memory_free_mb"`
	Temperature   float32 `json:"temperature"`
	UptimeSeconds int64   `json:"uptime_seconds"`
}

type AlertRecord struct {
	ID          int64  `json:"id"`
	DeviceID    string `json:"device_id"`
	Timestamp   int64  `json:"timestamp"`
	EventType   string `json:"event_type"`
	Description string `json:"description"`
}

func (r *Repository) SaveMessage(msg *v1.DeviceMessage) error {
	if r == nil || r.db == nil {
		return fmt.Errorf("repository or db is nil")
	}
	if msg == nil {
		return fmt.Errorf("device message is nil")
	}

	switch payload := msg.Payload.(type) {
	case *v1.DeviceMessage_Telemetry:
		if payload.Telemetry == nil {
			return fmt.Errorf("telemetry payload is nil")
		}
		_, err := r.db.Exec(`
			INSERT INTO telemetry (
				device_id,
				timestamp,
				cpu_usage,
				memory_free,
				temperature,
				uptime_seconds
			)
			VALUES ($1,$2,$3,$4,$5,$6)
		`,
			msg.GetDeviceId(),
			msg.GetTimestamp(),
			payload.Telemetry.CpuUsagePercent,
			payload.Telemetry.MemoryFreeBytes,
			payload.Telemetry.TemperatureCelsius,
			payload.Telemetry.UptimeSeconds,
		)
		return err

	case *v1.DeviceMessage_Alert:
		if payload.Alert == nil {
			return fmt.Errorf("alert payload is nil")
		}
		_, err := r.db.Exec(`
			INSERT INTO alerts (
				device_id,
				timestamp,
				event_type,
				description
			)
			VALUES ($1,$2,$3,$4)
		`,
			msg.GetDeviceId(),
			msg.GetTimestamp(),
			payload.Alert.EventType,
			payload.Alert.Description,
		)
		return err

	default:
		return fmt.Errorf("unsupported payload type %T", msg.Payload)
	}
}

func (r *Repository) GetLatestTelemetry(ctx context.Context, deviceID string) (*LatestTelemetry, error) {
	query := `
		SELECT id, device_id, timestamp, cpu_usage, memory_free, temperature, uptime_seconds
		FROM telemetry
	`
	var args []any
	if deviceID != "" {
		query += ` WHERE device_id = $1`
		args = append(args, deviceID)
	}
	query += ` ORDER BY timestamp DESC, id DESC LIMIT 1`

	var item LatestTelemetry
	err := r.db.QueryRowContext(ctx, query, args...).Scan(
		&item.ID,
		&item.DeviceID,
		&item.Timestamp,
		&item.CPUUsage,
		&item.MemoryFree,
		&item.Temperature,
		&item.UptimeSeconds,
	)
	if err != nil {
		return nil, err
	}
	item.MemoryFreeMB = item.MemoryFree / (1024 * 1024)
	return &item, nil
}

func (r *Repository) GetRecentAlerts(ctx context.Context, limit int) ([]AlertRecord, error) {
	if limit <= 0 || limit > 200 {
		limit = 20
	}

	rows, err := r.db.QueryContext(ctx, `
		SELECT id, device_id, timestamp, event_type, description
		FROM alerts
		WHERE event_type <> 'PONG'
		ORDER BY id DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []AlertRecord
	for rows.Next() {
		var a AlertRecord
		if err := rows.Scan(&a.ID, &a.DeviceID, &a.Timestamp, &a.EventType, &a.Description); err != nil {
			return nil, err
		}
		result = append(result, a)
	}
	return result, rows.Err()
}
