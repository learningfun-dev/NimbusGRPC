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
	pb "github.com/learningfun-dev/NimbusGRPC/nimbus/proto"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/redisclient"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"
)

const (
	REDIS_CLIENT_STATUS_REPLAY = "REPLAYING"
	redisStatusKeySuffix       = "-status"
	redisLocationKeySuffix     = "-location"
	redisLastAckKeySuffix      = "-last-acked-offset"
	ackCheckInterval           = 200 * time.Millisecond
	maxInFlightMessages        = 500 // Max messages sent without an ACK before pausing
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
	// NEW: Store the topic partition info for this job.
	topicPartition kafka.TopicPartition
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

// newReplayJob creates the resources and starts the processor goroutine for a client.
func (rc *ReplayConsumer) newReplayJob(ctx context.Context, clientID string) *clientReplayJob {
	jobCtx, cancel := context.WithCancel(ctx)
	job := &clientReplayJob{
		clientID:         clientID,
		msgChannel:       make(chan *kafka.Message, maxInFlightMessages),
		ctx:              jobCtx,
		cancelFunc:       cancel,
		consumer:         rc.consumer,
		wg:               rc.wg,
		inFlightMessages: list.New(),
	}
	rc.wg.Add(1)
	go job.run()
	return job
}

// run is the dedicated worker goroutine for a single client.
func (j *clientReplayJob) run() {
	defer j.wg.Done()
	defer j.resumePartition() // Ensure partition is resumed on exit
	log.Info().Str("clientID", j.clientID).Msg("ReplayConsumer: Starting dedicated replay processor goroutine.")

	ackTicker := time.NewTicker(ackCheckInterval)
	defer ackTicker.Stop()

	for {
		select {
		case <-ackTicker.C:
			j.checkAcks()
		case msg, ok := <-j.msgChannel:
			if !ok {
				log.Info().Str("clientID", j.clientID).Msg("ReplayConsumer: Replay message channel closed. Shutting down processor.")
				return
			}
			j.handleMessage(msg)
		case <-j.ctx.Done():
			log.Info().Str("clientID", j.clientID).Msg("ReplayConsumer: Replay processor context canceled. Shutting down.")
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

// checkAcks periodically checks Redis for acknowledgements and commits offsets.
func (j *clientReplayJob) checkAcks() {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.inFlightMessages.Len() == 0 {
		return
	}

	ackKey := j.clientID + redisLastAckKeySuffix
	lastAckedStr, err := redisclient.GetKeyValue(context.Background(), ackKey)
	if err != nil && !errors.Is(err, redis.Nil) {
		log.Error().Err(err).Str("clientID", j.clientID).Msg("ReplayConsumer: Failed to check ACK status in Redis.")
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
		log.Info().Str("clientID", j.clientID).Int64("commitOffset", int64(lastCommittedMsg.TopicPartition.Offset)).Msg("ReplayConsumer: Batch ACKed. Committing offset.")
		if _, err := j.consumer.CommitMessage(lastCommittedMsg); err != nil {
			log.Fatal().Err(err).Str("clientID", j.clientID).Msg("ReplayConsumer: CRITICAL - Failed to commit offset after batch ACK.")
		}
	}

	// CORRECTED: This logic now correctly resumes the partition.
	if j.isPaused && j.inFlightMessages.Len() < (maxInFlightMessages/2) {
		log.Info().Str("clientID", j.clientID).Int("inFlight", j.inFlightMessages.Len()).Msg("ReplayConsumer: In-flight window has space. Resuming partition.")
		if err := j.consumer.Resume([]kafka.TopicPartition{j.topicPartition}); err != nil {
			log.Error().Err(err).Str("clientID", j.clientID).Msg("ReplayConsumer: Failed to resume partition")
		}
		j.isPaused = false
	}
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

	status, err := redisclient.GetKeyValue(ctx, clientID+redisStatusKeySuffix)
	if err != nil {
		return fmt.Errorf("failed to get client status from Redis: %w", err)
	}
	if status != REDIS_CLIENT_STATUS_REPLAY {
		j.cancelFunc() // Cancel this job as the client is no longer replaying.
		return fmt.Errorf("client '%s' is no longer in REPLAYING state", clientID)
	}

	podChannel, err := redisclient.GetKeyValue(ctx, clientID+redisLocationKeySuffix)
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
