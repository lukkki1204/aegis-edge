package transport

import (
	"context"
	"io"
	"log"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"time"

	v1 "github.com/twojanazwa/aegis-edge/api/proto/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client manages the gRPC connection and bidirectional stream to cloud-gateway.
type Client struct {
	target string
	conn   *grpc.ClientConn
	client v1.AegisServiceClient

	mu     sync.Mutex
	stream v1.AegisService_ConnectClient
}

// NewClient dials the cloud gateway using insecure credentials for local/dev setups.
func NewClient(target string) (*Client, error) {
	conn, err := grpc.NewClient(
		target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}

	return &Client{
		target: target,
		conn:   conn,
		client: v1.NewAegisServiceClient(conn),
	}, nil
}

// Close closes the underlying gRPC connection.
func (c *Client) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// OpenStream opens a bidirectional Connect stream and spawns a command listener goroutine.
func (c *Client) OpenStream(ctx context.Context, onCommand func(*v1.CloudCommand)) (v1.AegisService_ConnectClient, error) {
	stream, err := c.client.Connect(ctx)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	c.stream = stream
	c.mu.Unlock()

	go c.listenCommands(ctx, stream, onCommand)
	return stream, nil
}

// ClearStreamIfMatch clears the stored stream pointer only when it matches the active stream.
func (c *Client) ClearStreamIfMatch(stream v1.AegisService_ConnectClient) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stream == stream {
		c.stream = nil
	}
}

func (c *Client) listenCommands(
	ctx context.Context,
	stream v1.AegisService_ConnectClient,
	onCommand func(*v1.CloudCommand),
) {
	defer c.ClearStreamIfMatch(stream)

	for {
		cmd, err := stream.Recv()
		if err == io.EOF {
			log.Println("Command stream closed by server (EOF).")
			return
		}
		if err != nil {
			if ctx.Err() == nil {
				log.Printf("Error receiving command from stream: %v", err)
			}
			return
		}

		if onCommand != nil {
			onCommand(cmd)
		}
	}
}

// ParseIntervalCommand extracts a valid telemetry interval from a SET_INTERVAL command.
func ParseIntervalCommand(cmd *v1.CloudCommand) (time.Duration, bool) {
	if cmd == nil || cmd.Action != "SET_INTERVAL" {
		return 0, false
	}

	raw, ok := cmd.Parameters["interval_seconds"]
	if !ok {
		return 0, false
	}

	seconds, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || seconds < 1 {
		log.Printf("Invalid interval_seconds value %q: %v", raw, err)
		return 0, false
	}

	return time.Duration(seconds) * time.Second, true
}

// NextBackoffWithJitter computes exponential backoff with +/-20% jitter.
func NextBackoffWithJitter(current, maxDelay time.Duration) (nextBase time.Duration, delay time.Duration) {
	nextBase = current * 2
	if nextBase > maxDelay {
		nextBase = maxDelay
	}

	jitterFactor := 0.8 + rand.Float64()*0.4
	delay = time.Duration(float64(nextBase) * jitterFactor)

	if delay < 100*time.Millisecond {
		delay = 100 * time.Millisecond
	}
	if delay > maxDelay {
		delay = maxDelay
	}

	return nextBase, delay
}
