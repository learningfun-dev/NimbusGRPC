package kafkaconsumer

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/learningfun-dev/NimbusGRPC/nimbus/config"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/kafkaadmin"
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

// checkAndTransitionClients implements the "Dual Lag Check" pattern.
func (sm *StatusManager) checkAndTransitionClients(ctx context.Context) error {
	replayingClients, err := sm.findReplayingClients(ctx)
	if err != nil {
		return fmt.Errorf("could not scan for replaying clients: %w", err)
	}

	if len(replayingClients) == 0 {
		log.Println("[INFO] StatusManager: No clients are currently in REPLAYING state.")
		return nil
	}

	log.Printf("[INFO] StatusManager: Found %d clients in REPLAYING state. Checking Kafka consumer group lags...", len(replayingClients))

	for _, clientID := range replayingClients {
		// --- The New Logic ---
		// A client's replay is complete only when the lag for THEIR partition is zero
		// in BOTH the results topic (for new messages) AND the DLQ topic (for old messages).

		// Condition A: Check lag on the live results topic.
		resultsPartition, err := kafkaadmin.GetPartitionForClientKey(ctx, sm.appConfig.KafkaResultsTopic, clientID)
		if err != nil {
			log.Printf("[WARN] StatusManager: Could not get partition for client '%s' on results topic. Skipping.", clientID)
			continue
		}
		resultsLag, err := kafkaadmin.GetPartitionLag(ctx, "nimbus-results-publisher-group", sm.appConfig.KafkaResultsTopic, resultsPartition)
		if err != nil {
			log.Printf("[WARN] StatusManager: Could not get lag for client '%s' on results topic. Skipping. Error: %v", clientID, err)
			continue
		}

		// Condition B: Check lag on the DLQ topic.
		dlqPartition, err := kafkaadmin.GetPartitionForClientKey(ctx, sm.appConfig.KafkaDLQTopic, clientID)
		if err != nil {
			log.Printf("[WARN] StatusManager: Could not get partition for client '%s' on DLQ topic. Skipping.", clientID)
			continue
		}
		dlqLag, err := kafkaadmin.GetPartitionLag(ctx, "nimbus-replay-service-group", sm.appConfig.KafkaDLQTopic, dlqPartition)
		if err != nil {
			log.Printf("[WARN] StatusManager: Could not get lag for client '%s' on DLQ topic. Skipping. Error: %v", clientID, err)
			continue
		}

		log.Printf("[DEBUG] StatusManager: Client '%s' lags -> ResultsTopic: %d, DLQTopic: %d", clientID, resultsLag, dlqLag)

		// Decision: If both lags are zero, the system is fully caught up for this client.
		if resultsLag == 0 && dlqLag == 0 {
			log.Printf("[INFO] StatusManager: Replay for client '%s' is complete (both topic lags are 0). Transitioning status to LIVE.", clientID)
			err := sm.transitionClientToLive(ctx, clientID)
			if err != nil {
				log.Printf("[ERROR] StatusManager: Failed to transition client '%s' to LIVE: %v", clientID, err)
			}
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
