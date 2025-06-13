package kafkaconsumer

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/config"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/constants"
	pb "github.com/learningfun-dev/NimbusGRPC/nimbus/proto"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/redisclient"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"
)

const (
	ackCheckInterval    = 200 * time.Millisecond
	maxInFlightMessages = 500
)

// clientReplayJob now manages the state for pipelined processing.
type clientReplayJob struct {
	clientID         string
	msgChannel       chan *kafka.Message
	ctx              context.Context
	cancelFunc       context.CancelFunc
	consumer         *kafka.Consumer
	wg               *sync.WaitGroup
	mu               sync.Mutex
	isPaused         bool
	inFlightMessages *list.List
	topicPartition   kafka.TopicPartition
	// NEW: Store the target offset for this client's replay session.
	targetOffset int64
}

// ReplayConsumer now dispatches work to more intelligent, pipelined workers.
type ReplayConsumer struct {
	consumer      *kafka.Consumer
	appConfig     *config.Config
	wg            *sync.WaitGroup
	shutdownCh    chan struct{}
	activeReplays sync.Map
}

// NewReplayConsumer creates a new ReplayConsumer.
func NewReplayConsumer(cfg *config.Config, wg *sync.WaitGroup) (*ReplayConsumer, error) {
	if cfg.KafkaDLQTopic == "" {
		return nil, fmt.Errorf("ReplayConsumer: KafkaDLQTopic is not configured")
	}
	consumerConfig := &kafka.ConfigMap{
		"bootstrap.servers":  cfg.KafkaBrokers,
		"group.id":           "nimbus-replay-service-group",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "false",
	}
	c, err := kafka.NewConsumer(consumerConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create ReplayConsumer: %w", err)
	}
	log.Info().Str("topic", cfg.KafkaDLQTopic).Msg("ReplayConsumer: Subscribing to topic")
	err = c.SubscribeTopics([]string{cfg.KafkaDLQTopic}, nil)
	if err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("failed to subscribe ReplayConsumer to topic %s: %w", cfg.KafkaDLQTopic, err)
	}
	return &ReplayConsumer{
		consumer:   c,
		appConfig:  cfg,
		wg:         wg,
		shutdownCh: make(chan struct{}),
	}, nil
}

// Start now acts as a simple, fast dispatcher.
func (rc *ReplayConsumer) Start(ctx context.Context) {
	defer rc.wg.Done()
	log.Info().Str("topic", rc.appConfig.KafkaDLQTopic).Msg("ReplayConsumer: Starting dispatcher loop")

	run := true
	for run {
		select {
		case <-ctx.Done():
			log.Info().Msg("ReplayConsumer: Dispatcher shutting down.")
			run = false
		case <-rc.shutdownCh:
			log.Info().Msg("ReplayConsumer: Dispatcher shutting down.")
			run = false
		default:
			// CORRECTED: The ReadMessage call is now inside the default case, making it reachable.
			msg, err := rc.consumer.ReadMessage(1 * time.Second)
			if err != nil {
				if kerr, ok := err.(kafka.Error); ok && kerr.Code() == kafka.ErrTimedOut {
					continue
				}
				log.Error().Err(err).Msg("ReplayConsumer: Error reading message")
				continue
			}

			clientID := string(msg.Key)
			if clientID == "" {
				log.Warn().Msg("ReplayConsumer: Message in DLQ has no Key. Skipping and committing.")
				if _, commitErr := rc.consumer.CommitMessage(msg); commitErr != nil {
					log.Fatal().Err(commitErr).Msg("ReplayConsumer: Failed to commit skipped message")
				}
				continue
			}

			jobUntyped, _ := rc.activeReplays.LoadOrStore(clientID, rc.newReplayJob(ctx, clientID))
			job := jobUntyped.(*clientReplayJob)

			job.msgChannel <- msg
		}
	}
}

// newReplayJob now fetches the target offset when it's created.
func (rc *ReplayConsumer) newReplayJob(ctx context.Context, clientID string) *clientReplayJob {
	jobCtx, cancel := context.WithCancel(ctx)

	// --- NEW LOGIC: Get the "finish line" offset when the job starts. ---
	targetOffsetStr, err := redisclient.GetKeyValue(ctx, clientID+constants.RedisTargetOffsetKeySuffix)
	if err != nil {
		log.Error().Err(err).Str("clientID", clientID).Msg("Could not get target offset for new replay job. Replay may not complete.")
		// We still create the job, but it will likely never finish without a target.
		// A robust system might re-queue this client for setup.
	}
	targetOffset, _ := strconv.ParseInt(targetOffsetStr, 10, 64)

	job := &clientReplayJob{
		clientID:         clientID,
		msgChannel:       make(chan *kafka.Message, maxInFlightMessages),
		ctx:              jobCtx,
		cancelFunc:       cancel,
		consumer:         rc.consumer,
		wg:               rc.wg,
		inFlightMessages: list.New(),
		targetOffset:     targetOffset, // Store the finish line
	}

	rc.wg.Add(1)
	go job.run()
	return job
}

// run is the dedicated worker goroutine for a single client.
func (j *clientReplayJob) run() {
	defer j.wg.Done()
	defer j.resumePartition()
	log.Info().Str("clientID", j.clientID).Int64("targetOffset", j.targetOffset).Msg("Starting dedicated replay processor goroutine.")

	ackTicker := time.NewTicker(ackCheckInterval)
	defer ackTicker.Stop()

	for {
		select {
		case <-ackTicker.C:
			j.checkAcksAndCompleteReplay() // Modified function name
		case msg, ok := <-j.msgChannel:
			if !ok {
				return
			}
			j.handleMessage(msg)
		case <-j.ctx.Done():
			return
		}
	}
}

// handleMessage sends a message and manages the in-flight window for backpressure.
func (j *clientReplayJob) handleMessage(msg *kafka.Message) {
	j.mu.Lock()
	defer j.mu.Unlock()

	// NEW: Store the partition info so we can always resume it.
	j.topicPartition = msg.TopicPartition

	if j.inFlightMessages.Len() >= maxInFlightMessages && !j.isPaused {
		log.Warn().Str("clientID", j.clientID).Int("inFlight", j.inFlightMessages.Len()).Msg("ReplayConsumer: In-flight message window is full. Pausing partition.")
		if err := j.consumer.Pause([]kafka.TopicPartition{j.topicPartition}); err != nil {
			log.Error().Err(err).Str("clientID", j.clientID).Msg("ReplayConsumer: Failed to pause partition")
		}
		j.isPaused = true
	}

	j.inFlightMessages.PushBack(msg)
	if err := j.publishMessageToRedis(msg); err != nil {
		log.Error().Err(err).Str("clientID", j.clientID).Msg("ReplayConsumer: Failed to publish message in batch to Redis.")
	}
}

// checkAcksAndCompleteReplay is the new core logic.
// It checks ACKs, commits offsets, and determines if the replay is finished.
func (j *clientReplayJob) checkAcksAndCompleteReplay() {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.inFlightMessages.Len() == 0 {
		return
	}

	ackKey := j.clientID + constants.RedisLastAckedOffsetKeySuffix
	lastAckedStr, err := redisclient.GetKeyValue(context.Background(), ackKey)
	if err != nil && !errors.Is(err, redis.Nil) {
		log.Error().Err(err).Str("clientID", j.clientID).Msg("Failed to check ACK status in Redis.")
		return
	}
	lastAckedOffset, _ := strconv.ParseInt(lastAckedStr, 10, 64)

	var lastCommittedMsg *kafka.Message
	for e := j.inFlightMessages.Front(); e != nil; {
		next := e.Next()
		msg := e.Value.(*kafka.Message)
		if int64(msg.TopicPartition.Offset) <= lastAckedOffset {
			lastCommittedMsg = msg
			j.inFlightMessages.Remove(e)
		} else {
			break
		}
		e = next
	}

	if lastCommittedMsg != nil {
		log.Info().Str("clientID", j.clientID).Int64("commitOffset", int64(lastCommittedMsg.TopicPartition.Offset)).Msg("Batch ACKed. Committing offset.")
		if _, err := j.consumer.CommitMessage(lastCommittedMsg); err != nil {
			log.Fatal().Err(err).Str("clientID", j.clientID).Msg("ReplayConsumer: CRITICAL - Failed to commit offset after batch ACK.")
		}

		// After a successful commit, check if we have reached the finish line.
		if int64(lastCommittedMsg.TopicPartition.Offset) >= j.targetOffset {
			log.Info().
				Str("clientID", j.clientID).
				Int64("lastAckedOffset", lastAckedOffset).
				Int64("targetOffset", j.targetOffset).
				Msg("Replay for client is complete. Transitioning status to LIVE.")

			// Transition status to LIVE and cleanup.
			j.completeReplay()

			// Cancel this worker's context to shut it down gracefully.
			j.cancelFunc()
		}
	}

	if j.isPaused && j.inFlightMessages.Len() < (maxInFlightMessages/2) {
		log.Info().Str("clientID", j.clientID).Int("inFlight", j.inFlightMessages.Len()).Msg("In-flight window has space. Resuming partition.")
		if err := j.consumer.Resume([]kafka.TopicPartition{j.topicPartition}); err != nil {
			log.Error().Err(err).Str("clientID", j.clientID).Msg("Failed to resume partition")
		}
		j.isPaused = false
	}
}

// completeReplay updates Redis to finalize the replay process.
func (j *clientReplayJob) completeReplay() {
	ctx := context.Background()
	statusKey := j.clientID + constants.RedisStatusKeySuffix
	targetOffsetKey := j.clientID + constants.RedisTargetOffsetKeySuffix
	lastAckedOffsetKey := j.clientID + constants.RedisLastAckedOffsetKeySuffix

	if err := redisclient.SetKeyValue(ctx, statusKey, constants.REDIS_CLIENT_STATUS_LIVE); err != nil {
		log.Error().Err(err).Str("clientID", j.clientID).Msg("Failed to transition client to LIVE")
		return
	}

	// Cleanup the state keys for this replay session.
	_ = redisclient.DeleteKey(ctx, targetOffsetKey)
	_ = redisclient.DeleteKey(ctx, lastAckedOffsetKey)
}

// resumePartition is a helper to ensure the partition is resumed when the goroutine exits.
func (j *clientReplayJob) resumePartition() {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.isPaused {
		log.Info().Str("clientID", j.clientID).Msg("ReplayConsumer: Resuming partition on shutdown.")
		_ = j.consumer.Resume([]kafka.TopicPartition{j.topicPartition})
	}
}

// publishMessageToRedis prepares and sends one replayed message to Redis.
func (j *clientReplayJob) publishMessageToRedis(msg *kafka.Message) error {
	var eventResp pb.KafkaEventResponse
	if err := proto.Unmarshal(msg.Value, &eventResp); err != nil {
		return fmt.Errorf("unmarshal error on DLQ message: %w", err)
	}

	ctx := context.Background()
	clientID := eventResp.ClientId

	status, err := redisclient.GetKeyValue(ctx, clientID+constants.RedisStatusKeySuffix)
	if err != nil {
		return fmt.Errorf("failed to get client status from Redis: %w", err)
	}
	if status != constants.REDIS_CLIENT_STATUS_REPLAY {
		j.cancelFunc() // Cancel this job as the client is no longer replaying.
		return fmt.Errorf("client '%s' is no longer in REPLAYING state", clientID)
	}

	podChannel, err := redisclient.GetKeyValue(ctx, clientID+constants.RedisLocationKeySuffix)
	if err != nil {
		return fmt.Errorf("client '%s' has no location in Redis: %w", clientID, err)
	}

	eventResp.RedisChannel = podChannel
	eventResp.KafkaOffset = int64(msg.TopicPartition.Offset)

	return redisclient.Publish(ctx, &eventResp)
}

// Shutdown gracefully stops the consumer.
func (rc *ReplayConsumer) Shutdown() {
	log.Info().Str("topic", rc.appConfig.KafkaDLQTopic).Msg("ReplayConsumer: Initiating shutdown...")
	rc.activeReplays.Range(func(key, value interface{}) bool {
		if job, ok := value.(*clientReplayJob); ok {
			job.cancelFunc()
		}
		return true
	})
	close(rc.shutdownCh)
	if rc.consumer != nil {
		_ = rc.consumer.Close()
	}
	log.Info().Str("topic", rc.appConfig.KafkaDLQTopic).Msg("ReplayConsumer: Shutdown complete.")
}
