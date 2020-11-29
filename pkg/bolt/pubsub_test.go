package boltwatermill

import (
	"fmt"

	"testing"

	"github.com/ThreeDotsLabs/watermill"

	"sync"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"

	_ "github.com/lib/pq"
	"go.etcd.io/bbolt"
)

var (
	logger = watermill.NewStdLogger(true, true)

	db *bbolt.DB

	closemu sync.RWMutex
)

func newPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	closemu.RLock()
	dbStormPub, dbStormSub := newstormPubSub(db, "file_test.db")
	closemu.RUnlock()
	publisher, err := NewPublisher(
		dbStormPub,
		PublisherConfig{SchemaAdapter: BoltSchemaAdapter{db: dbStormPub}},
		logger,
	)

	//require.NoError(t, err)

	subscriber, err := NewSubscriber(dbStormSub, SubscriberConfig{SchemaAdapter: BoltSchemaAdapter{db: dbStormSub}}, logger)
	//*storm.DB does not implement message.Subscriber (missing Subscribe method)
	//require.NoError(t, err)

	return publisher, subscriber
}

//ConsumerGroups is true, if consumer groups are supported, falce if consumer groups is not taken into account.
//here for the ConsumerGroups the result is false.
//ExactlyOnceDelivery true, if one-time delivery is supported, here it is true.
//GuaranteeOrder true, if the order of messages is guaranteed.
//Persistent true, if messages are persistent between multiple instances of a Pub / Sub

func TestPublishSubscribeBolt(t *testing.T) {
	features := tests.Features{
		ConsumerGroups:      false,
		ExactlyOnceDelivery: false,
		GuaranteedOrder:     false,
		Persistent:          false,
	}

	tests.TestPubSub(
		t,
		features,
		newPubSub,
		nil,
	)
}

/*
func configsMessage(db *storm.DB) error {
	var d message.Message
	msg := boltmessage{"config", 0, "emptytopic", time.Time{}, "", message.Message{Payload: d.Payload, UUID: d.UUID}}
	if dbwrite != nil {
		err := dbwrite.Save(&msg)
		if err != nil {
			return fmt.Errorf("not save message, %v", err)
		}
		fmt.Println("config saved")
	} else {
		return fmt.Errorf("db not initialized for configsMessage")
	}
	return nil
}
*/
