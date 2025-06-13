package kafkaconsumer

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/learningfun-dev/NimbusGRPC/nimbus/config"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/redisclient"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

const (
	statusCheckInterval = 10 * time.Second // Check more frequently to flip status faster

	// Key suffixes for our offset-based Redis state management.
	redisTargetOffsetKeySuffix    = "-replay-target-offset"
	redisLastAckedOffsetKeySuffix = "-last-acked-offset"
)

// StatusManager periodically checks for clients in REPLAYING state
// and transitions them back to LIVE when their replay is complete based on Kafka offsets.
type StatusManager struct {
	redisClient *redis.Client
	appConfig   *config.Config
	wg          *sync.WaitGroup
	shutdownCh  chan struct{}
}

// NewStatusManager creates a new StatusManager.
func NewStatusManager(cfg *config.Config, wg *sync.WaitGroup) (*StatusManager, error) {
	return &StatusManager{
		redisClient: redisclient.GetClient(),
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
			log.Info().Msg("StatusManager: Running offset-based replay completion check...")
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

// checkAndTransitionClients implements the "Offset-Based Replay Completion" pattern.
func (sm *StatusManager) checkAndTransitionClients(ctx context.Context) error {
	replayingClients, err := sm.findReplayingClients(ctx)
	if err != nil {
		return fmt.Errorf("could not scan for replaying clients: %w", err)
	}

	if len(replayingClients) == 0 {
		log.Info().Msg("StatusManager: No clients are currently in REPLAYING state.")
		return nil
	}

	log.Info().Int("count", len(replayingClients)).Msg("StatusManager: Found clients in REPLAYING state. Checking offset progress...")

	for _, clientID := range replayingClients {
		// --- THE CORRECT, DETERMINISTIC LOGIC ---
		// A client's replay is complete only when the last acknowledged offset
		// has reached the target offset that was set when the replay began.

		// Get the target offset (the "finish line").
		targetOffsetStr, err := redisclient.GetKeyValue(ctx, clientID+redisTargetOffsetKeySuffix)
		if err != nil {
			// If the target key is missing, something went wrong during connection setup.
			// It's safer to not change the state and let it be investigated.
			log.Warn().Err(err).Str("clientID", clientID).Msg("StatusManager: Could not get target offset for replaying client. Skipping.")
			continue
		}
		targetOffset, _ := strconv.ParseInt(targetOffsetStr, 10, 64)

		// Get the last offset the client has acknowledged. If it doesn't exist yet, treat it as 0.
		lastAckedOffsetStr, err := redisclient.GetKeyValue(ctx, clientID+redisLastAckedOffsetKeySuffix)
		if err != nil && !errors.Is(err, redis.Nil) {
			log.Warn().Err(err).Str("clientID", clientID).Msg("StatusManager: Could not get last ACKed offset for replaying client. Skipping.")
			continue
		}
		lastAckedOffset, _ := strconv.ParseInt(lastAckedOffsetStr, 10, 64)

		log.Debug().
			Str("clientID", clientID).
			Int64("last_acked_offset", lastAckedOffset).
			Int64("target_offset", targetOffset).
			Msg("StatusManager: Client replay progress")

		// Decision: If the last acknowledged offset has reached the target, the replay is complete.
		if lastAckedOffset >= targetOffset {
			log.Info().Str("clientID", clientID).Msg("Replay for client is complete. Transitioning status to LIVE.")
			err := sm.transitionClientToLive(ctx, clientID)
			if err != nil {
				log.Error().Err(err).Str("clientID", clientID).Msg("StatusManager: Failed to transition client to LIVE")
			} else {
				// Cleanup the state keys for this replay session.
				_ = redisclient.DeleteKey(ctx, clientID+redisTargetOffsetKeySuffix)
				_ = redisclient.DeleteKey(ctx, clientID+redisLastAckedOffsetKeySuffix)
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
		keys, nextCursor, err := sm.redisClient.Scan(ctx, cursor, pattern, 100).Result() // Scan in batches
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
