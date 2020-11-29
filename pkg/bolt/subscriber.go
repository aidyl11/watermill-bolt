// Sources for https://watermill.io/docs/getting-started/
package boltwatermill

import (
	"context"

	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/asdine/storm/v3"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
)

var (
	ErrSubscriberClosed = errors.New("subscriber is closed")
)

type SubscriberConfig struct {
	ConsumerGroup string

	PollInterval time.Duration

	ResendInterval time.Duration

	RetryInterval time.Duration

	InitializeSchema bool
	// SchemaAdapter provides the schema-dependent queries and arguments for them, based on topic/message etc and for saving acks and offsets of consumers.
	SchemaAdapter SchemaAdapter
}

func (c *SubscriberConfig) setDefaults() {
	if c != nil {
		if c.PollInterval == 0 {
			c.PollInterval = 10 * time.Millisecond
		}
		if c.ResendInterval == 0 {
			c.ResendInterval = 10 * time.Millisecond
		}
		if c.RetryInterval == 0 {
			c.RetryInterval = 10 * time.Millisecond
		}
	}
}

func (c SubscriberConfig) validate() error {
	if c.PollInterval <= 0 {
		return errors.New("poll interval must be a positive duration")
	}
	if c.ResendInterval <= 0 {
		return errors.New("resend interval must be a positive duration")
	}
	if c.RetryInterval <= 0 {
		return errors.New("resend interval must be a positive duration")
	}

	return nil
}

type Subscriber struct {
	consumerIdBytes  []byte
	consumerIdString string
	db               *storm.DB
	config           SubscriberConfig

	subscribeWg *sync.WaitGroup
	closing     chan struct{}
	closed      bool

	logger watermill.LoggerAdapter
}

//need a functional storm.DB
func NewSubscriber(db *storm.DB, config SubscriberConfig, logger watermill.LoggerAdapter) (*Subscriber, error) { //ici le retour n'est pas bon
	if db == nil {
		return nil, errors.New("db is nil")
	}
	config.setDefaults()
	err := config.validate()
	if err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	if logger == nil {
		logger = watermill.NopLogger{}
	}
	_, id, err := newSubscriberID()
	if err != nil {
		return &Subscriber{}, errors.Wrap(err, "cannot generate subscriber id")
	}
	logger = logger.With(watermill.LogFields{"subscriber_id": id})
	sub := &Subscriber{

		db:     db,
		config: config,

		subscribeWg: &sync.WaitGroup{},
		closing:     make(chan struct{}),

		logger: logger,
	}

	return sub, nil
}

func newSubscriberID() ([]byte, string, error) {
	id := watermill.NewULID()
	idBytes, err := ulid.MustParseStrict(id).MarshalBinary()
	if err != nil {
		return nil, "", errors.Wrap(err, "cannot marshal subscriber id")
	}

	return idBytes, id, nil
}

func (s *Subscriber) Subscribe(ctx context.Context, topic string) (o <-chan *message.Message, err error) {
	if s.closed {
		return nil, ErrSubscriberClosed
	}

	if err = validateTopicName(topic); err != nil {
		return nil, err
	}
	if s.config.InitializeSchema {
		if err := s.SubscribeInitialize(topic); err != nil {
			return nil, err
		}
	}

	// the information about closing the subscriber is propagated through ctx
	ctx, cancel := context.WithCancel(ctx)
	out := make(chan *message.Message)

	s.subscribeWg.Add(1)
	go func() {
		s.consume(ctx, topic, out)

		close(out)

		cancel()

	}()

	return out, nil
}

func (s *Subscriber) consume(ctx context.Context, topic string, out chan *message.Message) {
	if s != nil {
		defer s.subscribeWg.Done()
		logger := s.logger.With(watermill.LogFields{
			"topic":         topic,
			"consumergroup": s.config.ConsumerGroup,
		})

		var sleepTime time.Duration = 0
		for {
			select {
			case <-s.closing:
				logger.Info("subscriber closing / consume call", nil)
				return
			case <-ctx.Done():
				logger.Info("context canceled", nil)
				return
			case <-time.After(sleepTime): // Wait if needed
				sleepTime = 0
			}

			messageUUID, noMsg, err := s.query(ctx, topic, out, logger)

			if noMsg {
				// wait until polling for the next message
				logger.Debug("No messages, waiting until next query", watermill.LogFields{
					"wait_time": s.config.PollInterval,
				})
			}
			sleepTime = s.config.PollInterval
			if err != nil {
				logger.Debug("debug", watermill.LogFields{
					"err":          err.Error(),
					"message_uuid": messageUUID})
			}
		}
	}
}

func (s *Subscriber) query(
	ctx context.Context,
	topic string,
	out chan *message.Message,
	logger watermill.LoggerAdapter,
) (messageUUID string, noMsg bool, err error) {

	offset, msg, err := s.config.SchemaAdapter.SelectQuery(topic)

	if err != nil {
		// wait until polling for the next message
		logger.Debug("No more messages, waiting until next query", watermill.LogFields{"wait_time": s.config.PollInterval})
		time.Sleep(s.config.PollInterval)
		return "", true, nil
	}

	logger = logger.With(watermill.LogFields{
		"msg_uuid": msg.UUID,
	})
	//logger.Trace("Received message 233", nil)

	acked := s.sendMessage(ctx, msg, out, logger)
	if acked {
		//logger.Trace("sendMessage 237", nil)
		ackQuery, _ := s.config.SchemaAdapter.AckMessageQuery(topic, offset, s.config.ConsumerGroup)
		if ackQuery != "" {
			logger.Trace("Executing ack message query", watermill.LogFields{
				"query": ackQuery,
			})
		}

	}

	return msg.UUID, false, nil
}

// sendMessages sends messages on the output channel.
func (s *Subscriber) sendMessage(
	ctx context.Context,
	msg *message.Message,
	out chan *message.Message,
	logger watermill.LoggerAdapter,
) (acked bool) {

	msgCtx, cancel := context.WithCancel(ctx)
	msg.SetContext(msgCtx)
	defer cancel()
	//fmt.Printf("log 270\n")
ResendLoop:
	for {
		select {
		case out <- msg:
		case <-s.closing:
			//logger.Trace("subscriber closing / resend loop", nil)
			return false

		case <-ctx.Done():
			//logger.Trace("context canceled", nil)
			return false
		}

		select {
		case <-msg.Acked():
			//logger.Trace("Message acked by subscriber", nil)
			return true

		case <-msg.Nacked():
			//message nacked, try resending
			//logger.Trace("Message nacked, resending", nil)
			msg = msg.Copy()

			if s.config.ResendInterval != 0 {
				time.Sleep(s.config.ResendInterval)
			}

			continue ResendLoop

		case <-s.closing:
			//logger.Trace("Discarding queued message, subscriber closing", nil)
			return false

		case <-ctx.Done():
			//logger.Trace("Discarding queued message, context canceled", nil)
			return false
		}
	}
}

func (s *Subscriber) Close() error {
	if s != nil {
		if s.closed {
			return nil
		}
		s.closed = true
		close(s.closing)
		s.subscribeWg.Wait()
		s.logger.Debug("Subscriber closed", nil)

		//JMB db close
		if s.db != nil {
			s.db.Close()
		}
		return nil
	}
	return errors.Errorf("Close called on empty Subscriber")
}

func (s *Subscriber) SubscribeInitialize(topic string) (err error) {
	s.config.SchemaAdapter, err = initializeBoltSchema(
		context.Background(),
		topic,
		s.logger,
		s.db)
	return
}
