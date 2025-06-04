package redisclient

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/learningfun-dev/NimbusGRPC/nimbus/config"
	"github.com/redis/go-redis/v9"
)

var (
	client     *redis.Client
	once       sync.Once
	initErr    error // To store any error during initialization
	closeMutex sync.Mutex
	isClosed   bool
)

const (
	pingTimeout = 5 * time.Second // Timeout for initial ping
)

// InitClient initializes the Redis client using the provided configuration.
// It should be called once at application startup.
func InitClient(cfg *config.Config) error {
	once.Do(func() {
		log.Printf("[INFO] RedisClient: Initializing with Address: %s, DB: %d", cfg.RedisAddress, 0) // using DB 0 for now
		// In a real scenario, cfg might also have RedisPassword and RedisDB fields.
		// For now, using defaults as in the original code.
		opts := &redis.Options{
			Addr:     cfg.RedisAddress,
			Password: "", // cfg.RedisPassword if you add it to config
			DB:       0,  // cfg.RedisDB if you add it to config
			// Add other options like PoolSize, DialTimeout, ReadTimeout, WriteTimeout for production
		}

		rdb := redis.NewClient(opts)

		// Ping the server to ensure connectivity.
		ctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
		defer cancel()

		statusCmd := rdb.Ping(ctx)
		if err := statusCmd.Err(); err != nil {
			initErr = fmt.Errorf("failed to connect to Redis at %s: %w", cfg.RedisAddress, err)
			log.Printf("[FATAL] RedisClient: %v", initErr)
			// rdb.Close() // Close the partially initialized client on error
			return
		}

		client = rdb
		isClosed = false
		log.Printf("[INFO] RedisClient: Successfully connected to Redis at %s", cfg.RedisAddress)
	})
	return initErr
}

// GetClient returns the singleton Redis client instance.
// It panics if InitClient has not been called successfully.
func GetClient() *redis.Client {
	closeMutex.Lock() // Protect against getting client while closing or after closed
	defer closeMutex.Unlock()

	if client == nil || isClosed {
		// This indicates a programming error: GetClient called before successful InitClient or after CloseClient.
		log.Fatal("[FATAL] RedisClient: GetClient called before successful initialization or after client was closed.")
		return nil // Should not be reached due to log.Fatal
	}
	if initErr != nil {
		log.Fatalf("[FATAL] RedisClient: GetClient called but initialization failed: %v", initErr)
		return nil // Should not be reached
	}
	return client
}

// CloseClient closes the Redis client connection.
// It should be called once during application shutdown.
func CloseClient() error {
	closeMutex.Lock()
	defer closeMutex.Unlock()

	if client == nil {
		log.Println("[INFO] RedisClient: Client not initialized or already closed.")
		return nil
	}
	if isClosed {
		log.Println("[INFO] RedisClient: Client already closed.")
		return nil
	}

	log.Println("[INFO] RedisClient: Closing connection...")
	err := client.Close()
	if err != nil {
		log.Printf("[ERROR] RedisClient: Failed to close connection: %v", err)
		return err
	}
	isClosed = true
	// client = nil // Optionally set to nil after closing
	log.Println("[INFO] RedisClient: Connection closed.")
	return nil
}
