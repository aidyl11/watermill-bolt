package boltwatermill

import (
	"sync"

	"github.com/asdine/storm/v3"
	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

var (
	ErrPublisherClosed = errors.New("publisher is closed")
)

type PublisherConfig struct {
	AutoInitializeSchema bool
	// SchemaAdapter provides the schema-dependent queries and arguments for them, based on topic/message etc and for saving acks and offsets of consumers.
	SchemaAdapter SchemaAdapter
}

func (c *PublisherConfig) setDefaults() {
}

// Publisher inserts the Messages
type Publisher struct {
	config PublisherConfig

	db        *storm.DB
	publishWg *sync.WaitGroup
	closeCh   chan struct{}
	closed    bool

	initializedTopics sync.Map
	logger            watermill.LoggerAdapter
}

//Create a new Publisher - need a valid Config file, a valid Logger
func NewPublisher(db *storm.DB, config PublisherConfig, logger watermill.LoggerAdapter) (*Publisher, error) {
	config.setDefaults()
	if db == nil {
		return nil, errors.New("db is nil")
	}

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	if config.AutoInitializeSchema /* && isTx(db)*/ {

		return nil, errors.New("tried to use AutoInitializeSchema" +
			"an ongoing transaction")
	}

	config.SchemaAdapter, _ = initializeBoltSchema(
		nil, //context.Background(),
		"void",
		logger,
		db)

	return &Publisher{
		config: config,
		db:     db,

		publishWg: new(sync.WaitGroup),
		closeCh:   make(chan struct{}),
		closed:    false,

		logger: logger,
	}, nil
}

//Do the Publish verb - a topic is mandatory ("" is ok), messages is an array of message.Message

func (p *Publisher) Publish(topic string, msgs ...*message.Message) error {

	if p.closed {
		return errors.New("publisher closed")
	}
	p.publishWg.Add(1)
	defer p.publishWg.Done()
	logFields := make(watermill.LogFields, 4)
	logFields["topic"] = topic
	// we use schema_adatpter_bolt
	var err error
	_, _, err = p.config.SchemaAdapter.InsertQuery(topic, msgs)
	p.logger.Trace("Inserting message to Bolt", watermill.LogFields{})

	if err != nil {
		return errors.Wrap(err, "could not insert message as Bolt record")
	}

	return nil

}

func uuidWtermill() string {

	b := watermill.NewUUID()
	return b
}

//Bolt need
func (p *Publisher) initializeSchema(topic string) error {
	if p != nil {
		if !p.config.AutoInitializeSchema {
			return nil
		}

		if _, ok := p.initializedTopics.Load(topic); ok {
			return nil
		}

		p.initializedTopics.Store(topic, struct{}{})
		return nil
	}
	return errors.Errorf("initializeSchema called on empty receiver !")
}

func (p *Publisher) Close() error {
	if p != nil {
		if p.closed {
			return nil
		}
		p.closed = true
		close(p.closeCh)
		p.publishWg.Wait()
		if p.db != nil {
			p.db.Close()
		}

		//p.db.Close()
		return nil
	}
	return errors.Errorf("Close called on empty Publisher")
}
