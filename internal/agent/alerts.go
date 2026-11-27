package agent

import (
	"fmt"
	"time"

	v1 "github.com/twojanazwa/aegis-edge/api/proto/v1"
)

// Treshold values for CPU usage, temperature and used memory.
const (
	CPUAlertThreshold  = 70.0
	TempAlertThreshold = 70.0
	MemAlertThreshold  = 500 * 1024 * 1024 // 500 MB
)

// BuildAlerts takes measured data and if treshold is exceeded "alerts" table is updated
func (r *Runner) BuildAlerts(
	cpu float32,
	mem uint64,
	temp float32,
) []*v1.DeviceMessage {

	var alerts []*v1.DeviceMessage

	if float64(cpu) >= CPUAlertThreshold {
		alerts = append(alerts, &v1.DeviceMessage{
			DeviceId:  r.cfg.DeviceID,
			Timestamp: time.Now().Unix(),
			Payload: &v1.DeviceMessage_Alert{
				Alert: &v1.AlertEvent{
					EventType: "HIGH_CPU",
					Description: fmt.Sprintf(
						"CPU usage %.1f%% exceeded %.0f%%",
						cpu,
						CPUAlertThreshold,
					),
				},
			},
		})
	}

	if float64(temp) >= TempAlertThreshold {
		alerts = append(alerts, &v1.DeviceMessage{
			DeviceId:  r.cfg.DeviceID,
			Timestamp: time.Now().Unix(),
			Payload: &v1.DeviceMessage_Alert{
				Alert: &v1.AlertEvent{
					EventType: "HIGH_TEMP",
					Description: fmt.Sprintf(
						"Temperature %.1fC exceeded %.0fC",
						temp,
						TempAlertThreshold,
					),
				},
			},
		})
	}

	if mem <= MemAlertThreshold {
		alerts = append(alerts, &v1.DeviceMessage{
			DeviceId:  r.cfg.DeviceID,
			Timestamp: time.Now().Unix(),
			Payload: &v1.DeviceMessage_Alert{
				Alert: &v1.AlertEvent{
					EventType: "LOW_MEMORY",
					Description: fmt.Sprintf(
						"Free memory %d MB below threshold",
						mem/(1024*1024),
					),
				},
			},
		})
	}

	return alerts
}
