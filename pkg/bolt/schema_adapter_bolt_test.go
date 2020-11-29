// schema_adapter_bolt_test.go
package boltwatermill

import (
	"fmt"
	"testing"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/asdine/storm/v3"

	"go.etcd.io/bbolt"
)

const fileTest = "file_test5.db"
const topicTest = "test1"

func TestInsertQuery(t *testing.T) {
	s := BoltSchemaAdapter{}
	dbbolt, _ := bbolt.Open(fileTest, 0666, &bbolt.Options{ReadOnly: false})
	s.db, _ = storm.Open(fileTest, storm.UseDB(dbbolt))
	lmsg := &message.Message{Payload: []byte("Payload Not Empty")}
	lmsg.Metadata = map[string]string{}
	messagesTestMetadata := map[string]string{}
	messagesTestMetadata["id1"] = "toto1"
	messagesTestMetadata["id2"] = "toto2"
	messagesTestMetadata["id3"] = "toto3"
	for index, element := range messagesTestMetadata {
		lmsg.Metadata.Set(index, element)
	}

	msgs := message.Messages{lmsg}
	s.InsertQuery(topicTest, msgs)
	s.db.Close()
	dbbolt.Close()
}

func TestSelectQuery(t *testing.T) {
	s := BoltSchemaAdapter{}
	dbbolt, _ := bbolt.Open(fileTest, 0666, &bbolt.Options{ReadOnly: false})
	s.db, _ = storm.Open(fileTest, storm.UseDB(dbbolt))
	offset, msg, err := s.SelectQuery(topicTest)
	//err := s.db.One("Topic", "test1", &d)
	fmt.Printf("SelectQuery, Payload value is %+v , Err : %+v, offset %d \n", msg, err, offset)
	offset, msg, err = s.SelectQuery("wrong")
	fmt.Printf("SelectQuery, Payload value is %+v , Err : %+v, offset %d \n", msg, err, offset)
	s.db.Close()
	dbbolt.Close()
}

func TestAckMessageQuery(*testing.T) {
	s := BoltSchemaAdapter{}
	dbbolt, _ := bbolt.Open(fileTest, 0666, &bbolt.Options{ReadOnly: false})
	s.db, _ = storm.Open(fileTest, storm.UseDB(dbbolt))
	uuid, err := s.AckMessageQuery(topicTest, 2, "")
	fmt.Printf("uuid %s, err %+v\n", uuid, err)
	s.db.Close()
	dbbolt.Close()
}
