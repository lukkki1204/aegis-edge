package agent

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	v1 "github.com/twojanazwa/aegis-edge/api/proto/v1"
	"github.com/twojanazwa/aegis-edge/internal/agent/hal"
	"github.com/twojanazwa/aegis-edge/internal/agent/storage"
	"github.com/twojanazwa/aegis-edge/internal/agent/transport"
)

type Config struct {
	Target          string
	DeviceID        string
	DefaultInterval time.Duration
	InitialBackoff  time.Duration
	MaxBackoff      time.Duration
	ReconnectDelay  time.Duration
}

type Runner struct {
	cfg        Config
	sampler    *hal.Sampler
	buffer     *storage.BoltBuffer
	grpcClient *transport.Client
}

type streamWriter struct {
	stream v1.AegisService_ConnectClient
	mu     sync.Mutex
}

func NewRunner(
	cfg Config,
	sampler *hal.Sampler,
	buffer *storage.BoltBuffer,
	grpcClient *transport.Client,
) *Runner {
	return &Runner{
		cfg:        cfg,
		sampler:    sampler,
		buffer:     buffer,
		grpcClient: grpcClient,
	}
}

func newStreamWriter(stream v1.AegisService_ConnectClient) *streamWriter {
	if stream == nil {
		return nil
	}
	return &streamWriter{stream: stream}
}

func (w *streamWriter) Send(msg *v1.DeviceMessage) error {
	if w == nil || w.stream == nil {
		return fmt.Errorf("stream writer has no active stream")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.stream.Send(msg)
}

func (r *Runner) Run(ctx context.Context) {
	pauseCh := make(chan bool, 1)
	intervalCh := make(chan time.Duration, 1)
	flushCh := make(chan struct{}, 1)
	reconnectCmdCh := make(chan struct{}, 1)
	pingCh := make(chan string, 1)

	onCommand := func(cmd *v1.CloudCommand) {
		r.handleCommand(cmd, pauseCh, intervalCh, flushCh, reconnectCmdCh, pingCh)
	}

	backoff := r.cfg.InitialBackoff
	stream, err := r.grpcClient.OpenStream(ctx, onCommand)
	writer := newStreamWriter(stream)

	var reconnectCh <-chan time.Time
	if err != nil {
		firstDelay := backoff
		backoff, _ = transport.NextBackoffWithJitter(backoff, r.cfg.MaxBackoff)
		log.Printf("Initial stream open failed (%v). Will retry in %s...", err, firstDelay)
		reconnectCh = time.After(firstDelay)
	} else {
		log.Println("Connected to cloud gateway! Starting telemetry stream with offline buffering...")
	}

	ticker := time.NewTicker(r.cfg.DefaultInterval)
	defer ticker.Stop()

	paused := false

	for {
		select {
		case <-ctx.Done():
			log.Println("Shutdown signal received. Stopping telemetry loop...")
			if stream != nil {
				if closeErr := stream.CloseSend(); closeErr != nil {
					log.Printf("Warning: failed closing gRPC send stream: %v", closeErr)
				}
			}
			r.grpcClient.ClearStreamIfMatch(stream)
			return

		case nextInterval := <-intervalCh:
			ticker.Reset(nextInterval)
			log.Printf("Telemetry interval updated to %s", nextInterval)

		case nextPaused := <-pauseCh:
			paused = nextPaused
			log.Printf("Telemetry paused=%v", paused)

		case <-flushCh:
			stream = r.handleForceFlush(stream, &reconnectCh, &backoff)
			writer = newStreamWriter(stream)

		case cmdID := <-pingCh:
			r.sendPong(writer, cmdID)

		case <-reconnectCmdCh:
			stream = r.handleReconnectCmd(stream, &reconnectCh, &backoff)
			writer = nil

		case <-reconnectCh:
			reconnectCh = nil
			if stream != nil {
				continue
			}

			newStream, openErr := r.grpcClient.OpenStream(ctx, onCommand)
			if openErr != nil {
				stream = r.scheduleRetry(nil, &reconnectCh, &backoff, fmt.Sprintf("Reconnect stream failed: %v", openErr))
				writer = nil
				continue
			}

			stream = newStream
			writer = newStreamWriter(stream)
			log.Println("gRPC stream re-established with cloud gateway.")
			if flushErr := r.buffer.Flush(stream); flushErr != nil {
				stream = r.scheduleRetry(stream, &reconnectCh, &backoff, fmt.Sprintf("Flush after reconnect failed: %v", flushErr))
				writer = nil
				continue
			}

			backoff = r.cfg.InitialBackoff

		case <-ticker.C:
			if paused {
				continue
			}
			stream = r.tickTelemetry(stream, &reconnectCh, &backoff)
			writer = newStreamWriter(stream)
		}
	}
}

func (r *Runner) tickTelemetry(
	stream v1.AegisService_ConnectClient,
	reconnectCh *<-chan time.Time,
	backoff *time.Duration,
) v1.AegisService_ConnectClient {
	cpuUsage, err := r.sampler.CPUUsagePercent()
	if err != nil {
		log.Printf("Warning: failed to read CPU usage: %v", err)
		cpuUsage = 0
	}

	memFree, err := r.sampler.MemoryFreeBytes()
	if err != nil {
		log.Printf("Warning: failed to read memory metrics: %v", err)
		memFree = 0
	}

	uptimeSec, err := r.sampler.UptimeSeconds()
	if err != nil {
		log.Printf("Warning: failed to read system uptime: %v", err)
		uptimeSec = 0
	}

	temp, err := r.sampler.TemperatureCelsius()
	if err != nil {
		log.Printf("Warning: failed to read CPU temperature: %v", err)
		temp = 0
	}

	msg := &v1.DeviceMessage{
		DeviceId:  r.cfg.DeviceID,
		Timestamp: time.Now().Unix(),
		Payload: &v1.DeviceMessage_Telemetry{
			Telemetry: &v1.TelemetryData{
				CpuUsagePercent:    float64(cpuUsage),
				MemoryFreeBytes:    memFree,
				TemperatureCelsius: temp,
				UptimeSeconds:      uptimeSec,
			},
		},
	}

	if stream == nil {
		log.Println("Stream unavailable. Storing telemetry in local BoltDB buffer...")
		_ = r.buffer.Store(msg)
		if *reconnectCh == nil {
			*reconnectCh = time.After(r.cfg.ReconnectDelay)
		}
		return nil
	}

	if flushErr := r.buffer.Flush(stream); flushErr != nil {
		log.Printf("Flush failed (%v). Resetting stream and storing message locally...", flushErr)
		r.grpcClient.ClearStreamIfMatch(stream)
		_ = r.buffer.Store(msg)
		if *reconnectCh == nil {
			*reconnectCh = time.After(r.cfg.ReconnectDelay)
		}
		return nil
	}

	if sendErr := stream.Send(msg); sendErr != nil {
		log.Printf("Send failed (%v). Resetting stream and storing message locally...", sendErr)
		r.grpcClient.ClearStreamIfMatch(stream)
		if storeErr := r.buffer.Store(msg); storeErr != nil {
			log.Printf("Warning: failed storing message in buffer: %v", storeErr)
		}
		if *reconnectCh == nil {
			*backoff = r.cfg.InitialBackoff
			*reconnectCh = time.After(*backoff)
		}
		return nil
	}

	alerts := r.BuildAlerts(cpuUsage, memFree, temp)

	for _, alert := range alerts {
		if err := stream.Send(alert); err != nil {
			log.Printf("Failed to send alert: %v", err)
		} else {
			log.Printf(
				"Alert sent -> %s",
				alert.GetAlert().EventType,
			)
		}
	}

	log.Printf("Telemetry sent live -> CPU: %.1f%%, RAM Free: %d MB, Temp: %.1f°C, Uptime: %ds",
		cpuUsage, memFree/(1024*1024), temp, uptimeSec)
	return stream
}
