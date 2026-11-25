package storage

import (
	"fmt"
	"log"
	"time"

	v1 "github.com/twojanazwa/aegis-edge/api/proto/v1"
	"go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
)

var telemetryBucket = []byte("offline_telemetry")

// BoltBuffer manages local persistent storage for offline telemetry using BoltDB.
type BoltBuffer struct {
	db *bbolt.DB
}

// NewBoltBuffer opens or creates a local BoltDB database file.
func NewBoltBuffer(dbPath string) (*BoltBuffer, error) {
	db, err := bbolt.Open(dbPath, 0600, &bbolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}

	err = db.Update(func(tx *bbolt.Tx) error {
		_, bucketErr := tx.CreateBucketIfNotExists(telemetryBucket)
		return bucketErr
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	return &BoltBuffer{db: db}, nil
}

// Store saves a failed message locally in BoltDB using a nanosecond timestamp key.
func (b *BoltBuffer) Store(msg *v1.DeviceMessage) error {
	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	return b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(telemetryBucket)
		key := fmt.Appendf(nil, "%d", time.Now().UnixNano())
		return bucket.Put(key, data)
	})
}

// Flush sends all stored offline messages through the active gRPC stream and deletes sent entries.
func (b *BoltBuffer) Flush(stream v1.AegisService_ConnectClient) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(telemetryBucket)
		cursor := bucket.Cursor()

		var keysToDelete [][]byte

		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			var msg v1.DeviceMessage
			if err := proto.Unmarshal(v, &msg); err != nil {
				log.Printf("Failed to unmarshal offline message: %v", err)
				continue
			}

			if err := stream.Send(&msg); err != nil {
				// Keep remaining entries buffered when stream send fails.
				return err
			}

			log.Printf("Flushed offline message (Timestamp: %d) to cloud", msg.Timestamp)
			keysToDelete = append(keysToDelete, k)
		}

		for _, k := range keysToDelete {
			if err := bucket.Delete(k); err != nil {
				log.Printf("Failed to delete flushed key from local DB: %v", err)
			}
		}

		return nil
	})
}

// Close closes the underlying BoltDB database.
func (b *BoltBuffer) Close() error {
	return b.db.Close()
}
