package redisclient

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

// SetKeyValue stores a simple key-value pair in Redis.
// In our architecture, this is used to register a client's location (e.g., key="client-123", value="results:pod-abc").
func SetKeyValue(ctx context.Context, key string, value interface{}) error {
	rdb := GetClient() // Get the initialized singleton client

	// Use the provided context from the caller, which may contain deadlines or cancellation signals.
	if ctx == nil {
		ctx = context.Background() // Fallback if no context is provided.
	}

	err := rdb.Set(ctx, key, value, 0).Err()
	if err != nil {
		log.Error().Err(err).Str("key", key).Msg("RedisRegistry: Failed to SET key")
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	log.Debug().Str("key", key).Interface("value", value).Msg("RedisRegistry: Successfully SET key")
	return nil
}

// GetKeyValue retrieves a value by its key from Redis.
// It specifically handles the case where a key does not exist by returning redis.Nil,
// allowing the caller to decide if a missing key is an error or an expected condition.
func GetKeyValue(ctx context.Context, key string) (string, error) {
	rdb := GetClient()

	if ctx == nil {
		ctx = context.Background()
	}

	value, err := rdb.Get(ctx, key).Result()
	if err != nil {
		// If the key does not exist, redis returns redis.Nil.
		// This is often an expected outcome (e.g., checking if a client is connected),
		// not a system error. We return it so the calling function can check for it.
		if errors.Is(err, redis.Nil) {
			log.Debug().Str("key", key).Msg("RedisRegistry: Key does not exist (redis.Nil).")
			return "", redis.Nil // Return the specific redis.Nil error.
		}
		// For any other error (e.g., connection issue), log it as a proper error.
		log.Error().Err(err).Str("key", key).Msg("RedisRegistry: Failed to GET key")
		return "", fmt.Errorf("failed to get key %s: %w", key, err)
	}

	log.Debug().Str("key", key).Msg("RedisRegistry: Successfully GET key")
	return value, nil
}

// DeleteKey removes a key from Redis.
// Used when a client disconnects to clear its location from the registry.
func DeleteKey(ctx context.Context, key string) error {
	rdb := GetClient()

	if ctx == nil {
		ctx = context.Background()
	}

	// rdb.Del returns the number of keys deleted. err will be nil if the command succeeds,
	// even if the key didn't exist (it will just return 0).
	err := rdb.Del(ctx, key).Err()
	if err != nil {
		log.Error().Err(err).Str("key", key).Msg("RedisRegistry: Failed to DEL key")
		return fmt.Errorf("failed to delete key %s: %w", key, err)
	}

	log.Debug().Str("key", key).Msg("RedisRegistry: Successfully executed DEL command")
	return nil
}
