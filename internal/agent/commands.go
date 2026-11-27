package agent

import (
	"fmt"
	"log"
	"time"

	v1 "github.com/twojanazwa/aegis-edge/api/proto/v1"
	"github.com/twojanazwa/aegis-edge/internal/agent/transport"
)

// Commands available that can be sent from gateway to agent
const (
	ActionSetInterval = "SET_INTERVAL"
	ActionPause       = "PAUSE"
	ActionResume      = "RESUME"
	ActionForceFlush  = "FORCE_FLUSH"
	ActionReconnect   = "RECONNECT"
	ActionPing        = "PING"
)

// pushLatest pushes always latest value to channel that lenght is 1
func pushLatest[T any](ch chan T, val T) {
	select {
	case ch <- val:
	default:
		select {
		case <-ch:
		default:
		}
		ch <- val
	}
}

// handleCommand depending on what cmd is sent uplods proper channel with proper value
func (r *Runner) handleCommand(
	cmd *v1.CloudCommand,
	pauseCh chan bool,
	intervalCh chan time.Duration,
	flushCh chan struct{},
	reconnectCmdCh chan struct{},
	pingCh chan string,
) {
	if cmd == nil {
		return
	}
	log.Printf("Received CloudCommand id=%s action=%s params=%v", cmd.CommandId, cmd.Action, cmd.Parameters)

	switch cmd.Action {
	case ActionSetInterval:
		if nextInterval, ok := transport.ParseIntervalCommand(cmd); ok {
			pushLatest(intervalCh, nextInterval)
		}
	case ActionPause:
		pushLatest(pauseCh, true)
	case ActionResume:
		pushLatest(pauseCh, false)
	case ActionForceFlush:
		pushLatest(flushCh, struct{}{})
	case ActionReconnect:
		pushLatest(reconnectCmdCh, struct{}{})
	case ActionPing:
		pushLatest(pingCh, cmd.CommandId)
	default:
		log.Printf("Unknown CloudCommand action: %s", cmd.Action)
	}
}

// scheduleRetry sets new backoff and pushes data to reconnectCh after delay
func (r *Runner) scheduleRetry(
	stream v1.AegisService_ConnectClient,
	reconnectCh *<-chan time.Time,
	backoff *time.Duration,
	reason string,
) v1.AegisService_ConnectClient {
	if stream != nil {
		r.grpcClient.ClearStreamIfMatch(stream)
	}
	var nextDelay time.Duration
	*backoff, nextDelay = transport.NextBackoffWithJitter(*backoff, r.cfg.MaxBackoff)
	log.Printf("%s (next retry in %s)", reason, nextDelay)
	*reconnectCh = time.After(nextDelay)
	return nil
}

// handleForceFlush calls Flush method immediately on current buffer
func (r *Runner) handleForceFlush(
	stream v1.AegisService_ConnectClient,
	reconnectCh *<-chan time.Time,
	backoff *time.Duration,
) v1.AegisService_ConnectClient {
	if stream == nil {
		log.Println("FORCE_FLUSH skipped: no active gRPC stream.")
		return nil
	}
	if err := r.buffer.Flush(stream); err != nil {
		return r.scheduleRetry(stream, reconnectCh, backoff, fmt.Sprintf("FORCE_FLUSH failed: %v", err))
	}
	log.Println("FORCE_FLUSH completed.")
	return stream
}

// handleReconnectCmd closes stream sets backoff and pushes data to reconnectCh
func (r *Runner) handleReconnectCmd(
	stream v1.AegisService_ConnectClient,
	reconnectCh *<-chan time.Time,
	backoff *time.Duration,
) v1.AegisService_ConnectClient {
	log.Println("RECONNECT command received. Resetting current stream...")
	if stream != nil {
		if closeErr := stream.CloseSend(); closeErr != nil {
			log.Printf("Warning: failed closing gRPC send stream on reconnect: %v", closeErr)
		}
		r.grpcClient.ClearStreamIfMatch(stream)
	}
	*backoff = r.cfg.InitialBackoff
	*reconnectCh = time.After(0)
	return nil
}

// sendPong sends a PONG response over the active gRPC stream in reply to a PING command.
func (r *Runner) sendPong(writer *streamWriter, cmdID string) {
	if writer == nil {
		log.Printf("PING received (cmd=%s), but no active stream to send PONG.", cmdID)
		return
	}

	msg := &v1.DeviceMessage{
		DeviceId:  r.cfg.DeviceID,
		Timestamp: time.Now().Unix(),
		Payload: &v1.DeviceMessage_Pong{
			Pong: &v1.PongEvent{
				CommandId: cmdID,
				Message:   "pong",
			},
		},
	}

	if err := writer.Send(msg); err != nil {
		log.Printf("Failed to send PONG for %s: %v", cmdID, err)
	}
}
