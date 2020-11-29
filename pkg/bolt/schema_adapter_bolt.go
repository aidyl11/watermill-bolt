// Sources for https://watermill.io/docs/getting-started/
package boltwatermill

import (
	"context"

	//"fmt"

	//"log"
	"encoding/json"
	"time"

	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/asdine/storm/v3"
	"github.com/asdine/storm/v3/q"
	"go.etcd.io/bbolt"
)

type boltmessage struct {
	//ID string
	ID            string    `json:"UUID"`
	Offset        int       `storm:"index,increment"`
	Topic         string    `storm:"index"`
	ProcessedWhen time.Time `storm:"index"`
	MetadataJSON  string
	message.Message
}

type SchemaAdapter interface {
	//Insert the messages related to the topic
	InsertQuery(topic string, msgs message.Messages) (string, []interface{}, error)
	//SelectQuery extracts the message related to the topic, the query is not marked as read
	SelectQuery(topic string) (offset int, msg *message.Message, err error)
	//AckMessageQuery marks a message as read for a given consumer group. return the query and the status of the query (to help debug)
	AckMessageQuery(topic string, offset int, ConsumerGroup string) (string, error)
}

func initializeBoltSchema(ctx context.Context, topic string, logger watermill.LoggerAdapter, db *storm.DB) (schemaAdapter SchemaAdapter, err error) {
	schemaAdapter = BoltSchemaAdapter{db: db}
	err = validateTopicName(topic)
	if err != nil {
		return schemaAdapter, err
	}

	//initializingQueries := schemaAdapter.SchemaInitializingQueries(topic)

	logger.Info("Initializing subscriber schema", watermill.LogFields{
		//"query": initializingQueries,
	})

	return schemaAdapter, nil
}

type BoltSchemaAdapter struct {
	db *storm.DB
}

func newstormPubSub(ldb *bbolt.DB, filename string) (dbwrite *storm.DB, dbread *storm.DB) {

	//dbwrite, err = storm.Open("file_test.db")
	if ldb == nil {
		var lerr error
		ldb, lerr = bbolt.Open(filename, 0666, &bbolt.Options{Timeout: 1 * time.Second})
		//globalcount++
		//fmt.Printf("globalCount %d\n", globalcount)
		if lerr == nil {
			dbwrite, lerr = storm.Open(filename, storm.UseDB(ldb))

			dbread, lerr = storm.Open(filename, storm.UseDB(ldb))
		}
		if lerr != nil {
			//panic("db storm pb")
		}
	}

	return
}

//perform Bolt insertion
func (s BoltSchemaAdapter) InsertQuery(topic string, msgs message.Messages) (string, []interface{}, error) {
	//insertQuery := fmt.Sprintf("blabla %s\n", topic)
	insertQuery := "Insert Query Bolt"
	var (
		d boltmessage
	)
	for _, msg := range msgs {
		if msg != nil {
			d.ID = msg.UUID
			if msg.UUID == "" {
				d.ID = uuidWtermill()
			}

			var err error
			var tmp []byte
			tmp, err = json.Marshal(msg.Metadata)
			d.MetadataJSON = string(tmp)
			msg := boltmessage{d.ID, 0, topic, time.Time{}, d.MetadataJSON, *msg}
			if s.db != nil {

				err = s.db.Save(&msg)
				if err != nil {
					return insertQuery, nil, errors.Wrapf(err, "cannot save  message %s", msg.UUID)
				}
				//JMB p.logger.Trace("Message sent to BOLT", logFields)
			} else {
				return insertQuery, nil, errors.Errorf("s.db not initialized !")
			}
		}
	}
	return insertQuery, nil, nil
}

//perform Bolt extraction
func (s BoltSchemaAdapter) SelectQuery(topic string) (offset int, msg *message.Message, err error) {
	var d boltmessage
	//find the Topic "test1"
	d.Payload = []byte{}

	query := s.db.Select(q.Eq("Topic", topic), q.Eq("ProcessedWhen", time.Time{}))
	err = query.First(&d)
	offset = d.Offset

	msg = message.NewMessage(string(d.ID), d.Payload)
	msg.Metadata = map[string]string{}
	if d.MetadataJSON != "" {
		err = json.Unmarshal([]byte(d.MetadataJSON), &msg.Metadata)
	}
	//msg.Metadata = d.Metadata
	return
}

func (s BoltSchemaAdapter) AckMessageQuery(topic string, offset int, ConsumerGroup string) (qUUID string, err error) {
	var d boltmessage
	query := s.db.Select(q.Eq("Topic", topic), q.Eq("Offset", offset))
	err = query.First(&d)
	if err == nil {
		qUUID = d.ID
		err = s.db.UpdateField(&boltmessage{ID: qUUID}, "ProcessedWhen", time.Now())

	}
	return
}
