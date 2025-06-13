package kafkaconsumer

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/learningfun-dev/NimbusGRPC/nimbus/config"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/kafkaadmin"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/redisclient"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

const (
	// The interval at which the status manager will run its checks.
	statusCheckInterval = 1 * time.Minute
)

// StatusManager periodically checks for clients stuck in the REPLAYING state
// and transitions them back to LIVE when their replay is complete based on Kafka offsets.
type StatusManager struct {
	// This service only needs a Redis client. Kafka Admin Client is used via the kafkaadmin package.
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
	log.Info().Dur("interval", statusCheckInterval).Msg("StatusManager: Starting periodic checks")
	ticker := time.NewTicker(statusCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			log.Info().Msg("StatusManager: Running dual-lag replay completion check...")
			if err := sm.checkAndTransitionClients(ctx); err != nil {
				log.Error().Err(err).Msg("StatusManager: Failed during check cycle")
			}
		case <-ctx.Done():
			log.Info().Msg("StatusManager: Context cancelled. Shutting down.")
			return
		case <-sm.shutdownCh:
			log.Info().Msg("StatusManager: Shutdown signal received.")
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
		log.Info().Msg("StatusManager: No clients are currently in REPLAYING state.")
		return nil
	}

	log.Info().Int("count", len(replayingClients)).Msg("StatusManager: Found clients in REPLAYING state. Checking Kafka consumer group lags...")

	for _, clientID := range replayingClients {
		// Condition A: Check lag on the live results topic.
		resultsPartition, err := kafkaadmin.GetPartitionForClientKey(ctx, sm.appConfig.KafkaResultsTopic, clientID)
		if err != nil {
			log.Warn().Err(err).Str("clientID", clientID).Str("topic", sm.appConfig.KafkaResultsTopic).Msg("StatusManager: Could not get partition for results topic. Skipping.")
			continue
		}
		resultsLag, err := kafkaadmin.GetPartitionLag(ctx, "nimbus-results-publisher-group", sm.appConfig.KafkaResultsTopic, resultsPartition)
		if err != nil {
			log.Warn().Err(err).Str("clientID", clientID).Str("topic", sm.appConfig.KafkaResultsTopic).Msg("StatusManager: Could not get lag for results topic. Skipping.")
			continue
		}

		// Condition B: Check lag on the DLQ topic.
		dlqPartition, err := kafkaadmin.GetPartitionForClientKey(ctx, sm.appConfig.KafkaDLQTopic, clientID)
		if err != nil {
			log.Warn().Err(err).Str("clientID", clientID).Str("topic", sm.appConfig.KafkaDLQTopic).Msg("StatusManager: Could not get partition for DLQ topic. Skipping.")
			continue
		}
		dlqLag, err := kafkaadmin.GetPartitionLag(ctx, "nimbus-replay-service-group", sm.appConfig.KafkaDLQTopic, dlqPartition)
		if err != nil {
			log.Warn().Err(err).Str("clientID", clientID).Str("topic", sm.appConfig.KafkaDLQTopic).Msg("StatusManager: Could not get lag for DLQ topic. Skipping.")
			continue
		}

		log.Debug().
			Str("clientID", clientID).
			Int64("results_lag", resultsLag).
			Int64("dlq_lag", dlqLag).
			Msg("StatusManager: Client lags")

		// Decision: If both lags are zero, the system is fully caught up for this client.
		if resultsLag == 0 && dlqLag == 0 {
			log.Info().Str("clientID", clientID).Msg("Replay for client is complete (both topic lags are 0). Transitioning status to LIVE.")
			err := sm.transitionClientToLive(ctx, clientID)
			if err != nil {
				log.Error().Err(err).Str("clientID", clientID).Msg("StatusManager: Failed to transition client to LIVE")
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
		keys, nextCursor, err := sm.redisClient.Scan(ctx, cursor, pattern, 100).Result() // Scan in batches of 100
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
	log.Info().Msg("StatusManager: Initiating shutdown...")
	close(sm.shutdownCh)
	log.Info().Msg("StatusManager: Shutdown complete.")
}
