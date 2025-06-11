package kafkaconsumer

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/learningfun-dev/NimbusGRPC/nimbus/config"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/redisclient"
	"github.com/redis/go-redis/v9"
)

const (
	// The interval at which the status manager will run its checks.
	statusCheckInterval = 1 * time.Minute

	// Key suffixes for our new offset-based Redis state management.
	redisTargetOffsetKeySuffix    = "-replay-target-offset"
	redisLastAckedOffsetKeySuffix = "-last-acked-offset"
)

// StatusManager periodically checks for clients stuck in the REPLAYING state
// and transitions them back to LIVE when their replay is complete based on Kafka offsets.
type StatusManager struct {
	// The Kafka Admin Client is now used by the gRPC server pods to get the target offset.
	// This service only needs a Redis client.
	redisClient *redis.Client
	appConfig   *config.Config
	wg          *sync.WaitGroup
	shutdownCh  chan struct{}
}

// NewStatusManager creates a new StatusManager.
func NewStatusManager(cfg *config.Config, wg *sync.WaitGroup) (*StatusManager, error) {
	return &StatusManager{
		redisClient: redisclient.GetClient(), // Use the existing singleton Redis client
		appConfig:   cfg,
		wg:          wg,
		shutdownCh:  make(chan struct{}),
	}, nil
}

// Start begins the periodic check loop.
func (sm *StatusManager) Start(ctx context.Context) {
	defer sm.wg.Done()
	log.Printf("[INFO] StatusManager: Starting periodic checks every %v.", statusCheckInterval)
	ticker := time.NewTicker(statusCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			log.Println("[INFO] StatusManager: Running offset-based replay completion check...")
			if err := sm.checkAndTransitionClients(ctx); err != nil {
				log.Printf("[ERROR] StatusManager: Failed during check cycle: %v", err)
			}
		case <-ctx.Done():
			log.Println("[INFO] StatusManager: Context cancelled. Shutting down.")
			return
		case <-sm.shutdownCh:
			log.Println("[INFO] StatusManager: Shutdown signal received.")
			return
		}
	}
}

// checkAndTransitionClients implements the "Offset-Based Replay Completion" pattern.
func (sm *StatusManager) checkAndTransitionClients(ctx context.Context) error {
	// 1. Scan Redis for all clients in the REPLAYING state.
	replayingClients, err := sm.findReplayingClients(ctx)
	if err != nil {
		return fmt.Errorf("could not scan for replaying clients: %w", err)
	}

	if len(replayingClients) == 0 {
		log.Println("[INFO] StatusManager: No clients are currently in REPLAYING state.")
		return nil
	}

	log.Printf("[INFO] StatusManager: Found %d clients in REPLAYING state. Checking offset progress...", len(replayingClients))

	// 2. For each replaying client, compare their last ACKed offset to their target offset.
	for _, clientID := range replayingClients {
		targetOffsetKey := clientID + redisTargetOffsetKeySuffix
		lastAckedOffsetKey := clientID + redisLastAckedOffsetKeySuffix

		// Get the target offset (the "finish line").
		targetOffsetStr, err := redisclient.GetKeyValue(ctx, targetOffsetKey)
		if err != nil {
			log.Printf("[WARN] StatusManager: Could not get target offset for replaying client '%s'. Skipping. Error: %v", clientID, err)
			continue
		}
		targetOffset, _ := strconv.ParseInt(targetOffsetStr, 10, 64)

		// Get the last offset the client has acknowledged.
		lastAckedOffsetStr, err := redisclient.GetKeyValue(ctx, lastAckedOffsetKey)
		if err != nil && !errors.Is(err, redis.Nil) {
			log.Printf("[WARN] StatusManager: Could not get last ACKed offset for replaying client '%s'. Skipping. Error: %v", clientID, err)
			continue
		}
		lastAckedOffset, _ := strconv.ParseInt(lastAckedOffsetStr, 10, 64)

		// 3. Make the decision.
		if lastAckedOffset >= targetOffset {
			log.Printf("[INFO] StatusManager: Replay for client '%s' is complete (acked: %d >= target: %d). Transitioning status to LIVE.", clientID, lastAckedOffset, targetOffset)
			err := sm.transitionClientToLive(ctx, clientID)
			if err != nil {
				log.Printf("[ERROR] StatusManager: Failed to transition client '%s' to LIVE: %v", clientID, err)
			} else {
				// Cleanup the state keys for this replay session.
				_ = redisclient.DeleteKey(ctx, targetOffsetKey)
				_ = redisclient.DeleteKey(ctx, lastAckedOffsetKey)
			}
		} else {
			log.Printf("[DEBUG] StatusManager: Replay for client '%s' is still in progress (acked: %d < target: %d).", clientID, lastAckedOffset, targetOffset)
		}
	}

	return nil
}

// findReplayingClients scans Redis for keys matching '*-status' with a value of 'REPLAYING'.
func (sm *StatusManager) findReplayingClients(ctx context.Context) ([]string, error) {
	var clients []string
	var cursor uint64
	pattern := "*" + redisStatusKeySuffix

	for {
		keys, nextCursor, err := sm.redisClient.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return nil, err
		}

		for _, key := range keys {
			val, err := sm.redisClient.Get(ctx, key).Result()
			if err == nil && val == REDIS_CLIENT_STATUS_REPLAY {
				clientID := strings.TrimSuffix(key, redisStatusKeySuffix)
				clients = append(clients, clientID)
			}
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}
	return clients, nil
}

// transitionClientToLive safely updates a client's status from REPLAYING to LIVE.
func (sm *StatusManager) transitionClientToLive(ctx context.Context, clientID string) error {
	statusKey := clientID + redisStatusKeySuffix
	return redisclient.SetKeyValue(ctx, statusKey, REDIS_CLIENT_STATUS_LIVE)
}

// Shutdown gracefully stops the StatusManager.
func (sm *StatusManager) Shutdown() {
	log.Println("[INFO] StatusManager: Initiating shutdown...")
	close(sm.shutdownCh)
	// We no longer manage our own admin client, so no need to close it here.
	log.Println("[INFO] StatusManager: Shutdown complete.")
}
