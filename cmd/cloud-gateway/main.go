// Package main implements cloud-gateway responsible for communicating with agent.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	v1 "github.com/twojanazwa/aegis-edge/api/proto/v1"
	"github.com/twojanazwa/aegis-edge/internal/database"
	"github.com/twojanazwa/aegis-edge/internal/worker"
	"google.golang.org/grpc"
)

// Constant values used for setting specific command that agent receives.
const (
	ActionSetInterval = "SET_INTERVAL"
	ActionPause       = "PAUSE"
	ActionResume      = "RESUME"
	ActionForceFlush  = "FORCE_FLUSH"
	ActionReconnect   = "RECONNECT"
	ActionPing        = "PING"
)

// CommandSender wraps a gRPC stream and serializes outgoing cloud commands.
type commandSender struct {
	stream v1.AegisService_ConnectServer
	mu     sync.Mutex
}

// Initialization of new commandSender with provided active stream
func newCommandSender(stream v1.AegisService_ConnectServer) *commandSender {
	return &commandSender{stream: stream}
}

// Send method sends command to agent using commandSender
func (s *commandSender) Send(action string, params map[string]string) error {
	cmd := &v1.CloudCommand{
		CommandId:  fmt.Sprintf("cmd-%s-%d", action, time.Now().UnixNano()),
		Action:     action,
		Parameters: params,
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.stream.Send(cmd); err != nil {
		log.Printf("Failed to send %s (%s): %v", cmd.Action, cmd.CommandId, err)
		return err
	}

	log.Printf("Sent %s (id=%s)", cmd.Action, cmd.CommandId)
	return nil
}

// gatewayServer coordinates gRPC agent streams, PostgreSQL persistence, and admin HTTP endpoints.
type gatewayServer struct {
	v1.UnimplementedAegisServiceServer
	processor *worker.Processor
	repo      *worker.Repository
	db        interface{ Ping() error }

	mu           sync.RWMutex
	sender       *commandSender
	lastDeviceID string
}

func (s *gatewayServer) setLastDeviceID(deviceID string) {
	if deviceID == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastDeviceID = deviceID
}

func (s *gatewayServer) getLastDeviceID() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastDeviceID
}

// setSender sets new command server after starting new stream
func (s *gatewayServer) setSender(cs *commandSender) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sender = cs
}

// clearSender clears active stream
func (s *gatewayServer) clearSender(cs *commandSender) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sender == cs {
		s.sender = nil
	}
}

// getSender returns the active commandSender, or nil when no device is connected.
func (s *gatewayServer) getSender() *commandSender {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sender
}

// startAdminHTTP starts the administrative HTTP server for commands (/cmd) and REST status endpoints.
func (s *gatewayServer) startAdminHTTP(addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/cmd", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "use POST", http.StatusMethodNotAllowed)
			return
		}

		action := r.URL.Query().Get("action")
		if action == "" {
			http.Error(w, "missing ?action=", http.StatusBadRequest)
			return
		}

		params := map[string]string{}
		if interval := r.URL.Query().Get("interval_seconds"); interval != "" {
			params["interval_seconds"] = interval
		}

		cs := s.getSender()
		if cs == nil {
			http.Error(w, "no connected edge device", http.StatusServiceUnavailable)
			return
		}

		if err := cs.Send(action, params); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		var details string
		if action == "SET_INTERVAL" {
			details = fmt.Sprintf("Command SET_INTERVAL executed, interval changed to %s seconds", params["interval_seconds"])
		} else {
			details = fmt.Sprintf("Command %s executed.", action)
		}

		if err := s.repo.InsertEvent(r.Context(), s.getLastDeviceID(), action, details); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, "ok: %s\n", action)
	})

	mux.HandleFunc("/api/v1/health", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "use GET", http.StatusMethodNotAllowed)
			return
		}
		if err := s.db.Ping(); err != nil {
			http.Error(w, "db unreachable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	mux.HandleFunc("/api/v1/telemetry/latest", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "use GET", http.StatusMethodNotAllowed)
			return
		}
		deviceID := r.URL.Query().Get("device_id")
		item, err := s.repo.GetLatestTelemetry(r.Context(), deviceID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(item)
	})

	mux.HandleFunc("/api/v1/alerts", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "use GET", http.StatusMethodNotAllowed)
			return
		}
		limit := 20
		if rawLimit := r.URL.Query().Get("limit"); rawLimit != "" {
			if parsed, err := strconv.Atoi(rawLimit); err == nil {
				limit = parsed
			}
		}
		items, err := s.repo.GetRecentAlerts(r.Context(), limit)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(items)
	})

	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		log.Printf("Command HTTP endpoint listening on %s...", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("Admin HTTP server error: %v", err)
		}
	}()
}

// Connect handles the bidirectional gRPC stream from an edge agent.
func (s *gatewayServer) Connect(stream v1.AegisService_ConnectServer) error {
	log.Println("Edge device connected successfully.")

	sender := newCommandSender(stream)
	s.setSender(sender)
	defer s.clearSender(sender)

	// Receive messages from the agent and pass each payload type to Processor.
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			log.Println("Edge device closed the stream gracefully.")
			return nil
		}
		if err != nil {
			log.Printf("Lost connection with edge device (stream error): %v", err)
			return err
		}
		if msg.GetDeviceId() != "" {
			s.setLastDeviceID(msg.GetDeviceId())
		}

		switch payload := msg.Payload.(type) {
		case *v1.DeviceMessage_Telemetry:
			t := payload.Telemetry
			log.Printf("Telemetry [%s] -> CPU: %.1f%%, RAM Free: %d MB, Temp: %.1f°C, Uptime: %ds",
				msg.DeviceId, t.CpuUsagePercent, t.MemoryFreeBytes/(1024*1024), t.TemperatureCelsius, t.UptimeSeconds)
			if err := s.processor.Process(msg); err != nil {
				log.Printf("Failed to process telemetry: %v", err)
			}

		case *v1.DeviceMessage_Alert:
			a := payload.Alert
			log.Printf("ALERT [%s]: %s - %s", msg.DeviceId, a.EventType, a.Description)
			if err := s.processor.Process(msg); err != nil {
				log.Printf("Failed to process alert: %v", err)
			}
		case *v1.DeviceMessage_Pong:
			p := payload.Pong
			if p != nil {
				log.Printf("PONG [%s]: cmd=%s msg=%s", msg.DeviceId, p.CommandId, p.Message)
				if s.repo != nil {
					details := p.Message
					if p.CommandId != "" {
						details = fmt.Sprintf("cmd=%s msg=%s", p.CommandId, p.Message)
					}
					if err := s.repo.InsertEvent(stream.Context(), msg.GetDeviceId(), "PONG", details); err != nil {
						log.Printf("Failed to insert PONG event: %v", err)
					}
				}
			}
		}
	}
}

func main() {
	// Open and verify connection to PostgreSQL.
	db, err := database.NewPostgres(
		"host=localhost port=5432 user=aegis password=aegis dbname=aegis sslmode=disable",
	)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}
	log.Println("Connected to PostgreSQL!")

	// Wire repository and processor to persist incoming messages.
	repo := worker.NewRepository(db)
	processor := worker.NewProcessor(repo)

	gw := &gatewayServer{
		processor: processor,
		repo:      repo,
		db:        db,
	}
	gw.startAdminHTTP(":8081")

	// Start the gRPC server for edge devices.
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen on port 50051: %v", err)
	}

	grpcServer := grpc.NewServer()
	v1.RegisterAegisServiceServer(grpcServer, gw)

	log.Println("Cloud Gateway is listening on :50051...")

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("gRPC server error: %v", err)
	}
}
