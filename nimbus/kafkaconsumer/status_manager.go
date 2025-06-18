package kafkaconsumer

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/learningfun-dev/NimbusGRPC/nimbus/config"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/constants"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/kafkaadmin"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/redisclient"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

const (
	// The interval at which the coordinator will run its checks.
	statusCheckInterval = 1 * time.Minute
)

// StatusManager periodically checks for clients in REPLAYING state
// and transitions them to LIVE when all relevant Kafka queues are fully drained.
type StatusManager struct {
	redisClient *redis.Client
	appConfig   *config.Config
	wg          *sync.WaitGroup
	shutdownCh  chan struct{}
}

// NewStatusManager creates a new StatusManager.
func NewStatusManager(cfg *config.Config, wg *sync.WaitGroup) (*StatusManager, error) {
	// This service needs kafkaadmin to be initialized in its main.go
	if kafkaadmin.GetAdminClient() == nil {
		return nil, fmt.Errorf("StatusManager requires KafkaAdmin to be initialized first")
	}

	return &StatusManager{
		redisClient: redisclient.GetClient(),
		appConfig:   cfg,
		wg:          wg,
		shutdownCh:  make(chan struct{}),
	}, nil
}

// Start begins the periodic coordinator loop.
func (sm *StatusManager) Start(ctx context.Context) {
	defer sm.wg.Done()
	log.Info().Dur("interval", statusCheckInterval).Msg("StatusManager (Coordinator): Starting periodic checks")
	ticker := time.NewTicker(statusCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			log.Info().Msg("StatusManager (Coordinator): Running triple-lag replay completion check...")
			if err := sm.checkAndTransitionClients(ctx); err != nil {
				log.Error().Err(err).Msg("StatusManager (Coordinator): Failed during check cycle")
			}
		case <-ctx.Done():
			log.Info().Msg("StatusManager (Coordinator): Context cancelled. Shutting down.")
			return
		case <-sm.shutdownCh:
			log.Info().Msg("StatusManager (Coordinator): Shutdown signal received.")
			return
		}
	}
}

// checkAndTransitionClients implements the "Triple Lag Check" pattern.
func (sm *StatusManager) checkAndTransitionClients(ctx context.Context) error {
	replayingClients, err := sm.findReplayingClients(ctx)
	if err != nil {
		return fmt.Errorf("could not scan for replaying clients: %w", err)
	}

	if len(replayingClients) == 0 {
		log.Info().Msg("StatusManager (Coordinator): No clients are currently in REPLAYING state.")
		return nil
	}

	log.Info().Int("count", len(replayingClients)).Msg("StatusManager (Coordinator): Found clients in REPLAYING state. Checking Kafka consumer group lags...")

	for _, clientID := range replayingClients {
		isCaughtUp, err := sm.isClientCaughtUp(ctx, clientID)
		if err != nil {
			log.Warn().Err(err).Str("clientID", clientID).Msg("StatusManager (Coordinator): Could not determine if client is caught up. Skipping.")
			continue
		}

		if isCaughtUp {
			log.Info().Str("clientID", clientID).Msg("Replay for client is complete (all topic lags are 0). Transitioning status to LIVE.")
			err := sm.transitionClientToLive(ctx, clientID)
			if err != nil {
				log.Error().Err(err).Str("clientID", clientID).Msg("StatusManager (Coordinator): Failed to transition client to LIVE")
			}
		} else {
			log.Debug().Str("clientID", clientID).Msg("StatusManager (Coordinator): Client is not yet fully caught up across all topics.")
		}
	}

	return nil
}

// isClientCaughtUp performs the check on all three topics.
func (sm *StatusManager) isClientCaughtUp(ctx context.Context, clientID string) (bool, error) {
	// Condition #1: Check lag on the initial events topic.
	eventsLag, err := sm.getLagForTopic(ctx, clientID, sm.appConfig.KafkaEventsTopic, "nimbus-events-processor-group")
	if err != nil {
		return false, err
	}
	if eventsLag > 0 {
		log.Debug().Str("clientID", clientID).Int64("lag", eventsLag).Str("topic", sm.appConfig.KafkaEventsTopic).Msg("Lag detected in events topic.")
		return false, nil
	}

	// Condition #2: Check lag on the live results topic.
	resultsLag, err := sm.getLagForTopic(ctx, clientID, sm.appConfig.KafkaResultsTopic, "nimbus-results-publisher-group")
	if err != nil {
		return false, err
	}
	if resultsLag > 0 {
		log.Debug().Str("clientID", clientID).Int64("lag", resultsLag).Str("topic", sm.appConfig.KafkaResultsTopic).Msg("Lag detected in results topic.")
		return false, nil
	}

	// Condition #3: Check lag on the DLQ topic.
	dlqLag, err := sm.getLagForTopic(ctx, clientID, sm.appConfig.KafkaDLQTopic, "nimbus-replay-service-group")
	if err != nil {
		return false, err
	}
	if dlqLag > 0 {
		log.Debug().Str("clientID", clientID).Int64("lag", dlqLag).Str("topic", sm.appConfig.KafkaDLQTopic).Msg("Lag detected in DLQ topic.")
		return false, nil
	}

	// If all three lags are zero, the client is fully caught up.
	return true, nil
}

// getLagForTopic is a helper to get the lag for a specific client and topic.
func (sm *StatusManager) getLagForTopic(ctx context.Context, clientID, topic, groupID string) (int64, error) {
	partition, err := kafkaadmin.GetPartitionForClientKey(ctx, topic, clientID)
	if err != nil {
		return -1, fmt.Errorf("could not get partition for client '%s' on topic '%s': %w", clientID, topic, err)
	}
	lag, err := kafkaadmin.GetPartitionLag(ctx, groupID, topic, partition)
	if err != nil {
		return -1, fmt.Errorf("could not get lag for client '%s' on topic '%s' partition %d: %w", clientID, topic, partition, err)
	}
	return lag, nil
}

// findReplayingClients scans Redis for keys matching '*-status' with a value of 'REPLAYING'.
func (sm *StatusManager) findReplayingClients(ctx context.Context) ([]string, error) {
	var clients []string
	var cursor uint64
	pattern := "*" + constants.RedisStatusKeySuffix

	for {
		keys, nextCursor, err := sm.redisClient.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return nil, err
		}

		for _, key := range keys {
			val, err := sm.redisClient.Get(ctx, key).Result()
			if err == nil && val == constants.REDIS_CLIENT_STATUS_REPLAY {
				clientID := strings.TrimSuffix(key, constants.RedisStatusKeySuffix)
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
	statusKey := clientID + constants.RedisStatusKeySuffix
	return redisclient.SetKeyValue(ctx, statusKey, constants.REDIS_CLIENT_STATUS_LIVE)
}

// Shutdown gracefully stops the StatusManager.
func (sm *StatusManager) Shutdown() {
	log.Info().Msg("StatusManager (Coordinator): Initiating shutdown...")
	close(sm.shutdownCh)
	log.Info().Msg("StatusManager (Coordinator): Shutdown complete.")
}
