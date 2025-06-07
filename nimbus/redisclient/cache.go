package redisclient

import (
	"context"
	"fmt"
	"log"
)

// It uses the globally managed Redis client.
func SetKey(ctx context.Context, key string, value interface{}) error {
	rdb := GetClient() // Get the initialized singleton client

	// It's good practice to use the context passed from the caller,
	// or a background context if the operation is truly detached.
	if ctx == nil {
		ctx = context.Background() // Fallback, but prefer caller-supplied context
	}

	err := rdb.Set(ctx, key, value, 0).Err()
	if err != nil {
		log.Printf("[ERROR] redisClient.SetKey: Failed to set key-value, '%s': %v. error: %v", key, value, err)
		return fmt.Errorf("failed to set key-value pair %s: %w", key, err)
	}

	log.Printf("[DEBUG] redisClient.SetKey: key-value successfully saved to Redis '%s'. value: %s", key, value)
	return nil
}

func GeyKey(ctx context.Context, key string) (string, error) {
	rdb := GetClient() // Get the initialized singleton client

	// It's good practice to use the context passed from the caller,
	// or a background context if the operation is truly detached.
	if ctx == nil {
		ctx = context.Background() // Fallback, but prefer caller-supplied context
	}

	value, err := rdb.Get(ctx, key).Result()

	if err != nil {
		log.Printf("[ERROR] redisClient.GetKey: '%s' key doesnt exists", key)
		return "", err
	}

	log.Printf("[DEBUG] redisClient.GetKey: '%s' key retrived successfully.", key)
	return value, nil
}

func DelKey(ctx context.Context, key string) error {
	rdb := GetClient() // Get the initialized singleton client

	// It's good practice to use the context passed from the caller,
	// or a background context if the operation is truly detached.
	if ctx == nil {
		ctx = context.Background() // Fallback, but prefer caller-supplied context
	}

	err := rdb.Del(ctx, key).Err()
	if err != nil {
		log.Printf("[ERROR] redisClient.DeleteKey: Failed to delete key '%s': %v.", key, err)
		return fmt.Errorf("failed to del key %s: %w", key, err)
	}

	log.Printf("[DEBUG] redisClient.DeleteKey: successfully deleted '%s' from Redis .", key)
	return nil
}
