package redisclient

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/learningfun-dev/NimbusGRPC/nimbus/config"
	"github.com/rs/zerolog/log"

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
		log.Info().
			Str("address", cfg.RedisAddress).
			Int("db", 0).
			Msg("RedisClient: Initializing")

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
			log.Fatal().Err(initErr).Msg("RedisClient: Ping failed")
			return
		}

		client = rdb
		isClosed = false
		log.Info().Str("address", cfg.RedisAddress).Msg("RedisClient: Successfully connected to Redis")
	})
	return initErr
}

// GetClient returns the singleton Redis client instance.
// It panics if InitClient has not been called successfully.
func GetClient() *redis.Client {
	closeMutex.Lock() // Protect against getting client while closing or after closed
	defer closeMutex.Unlock()

	if client == nil || isClosed {
		log.Fatal().Msg("RedisClient: GetClient called before successful initialization or after client was closed.")
		return nil // Should not be reached due to log.Fatal
	}
	if initErr != nil {
		log.Fatal().Err(initErr).Msg("RedisClient: GetClient called but initialization failed")
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
		log.Info().Msg("RedisClient: Client not initialized or already closed.")
		return nil
	}
	if isClosed {
		log.Info().Msg("RedisClient: Client already closed.")
		return nil
	}

	log.Info().Msg("RedisClient: Closing connection...")
	err := client.Close()
	if err != nil {
		log.Error().Err(err).Msg("RedisClient: Failed to close connection")
		return err
	}
	isClosed = true
	log.Info().Msg("RedisClient: Connection closed.")
	return nil
}
