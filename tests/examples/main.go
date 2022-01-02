package main

import (
	"context"
	"database/sql"
	"log"

	"github.com/alexrudd/gomdb"
	"github.com/gofrs/uuid"
	_ "github.com/lib/pq"
)

// Message is an arbitrary message type.
type Message struct {
	AttributeA string
	AttributeB int64
}

func main() {
	// Open database connection
	db, err := sql.Open("postgres", "dbname=message_store sslmode=disable user=message_store")
	if err != nil {
		log.Fatalf("unexpected error opening db: %s", err)
	}
	defer db.Close()

	// Set search path for schema
	if _, err := db.Exec("SET search_path TO message_store,public;"); err != nil {
		log.Fatalf("setting search path: %s", err)
	}

	// create client
	client := gomdb.NewClient(db)
	stream := gomdb.StreamIdentifier{
		Category: "demo",
		ID:       uuid.Must(uuid.NewV4()).String(),
	}

	// write a message
	_, err = client.WriteMessage(
		context.Background(),
		stream,
		gomdb.ProposedMessage{
			ID:   uuid.Must(uuid.NewV4()).String(),
			Type: "DemoMessage",
			Data: &Message{
				AttributeA: "Demo",
				AttributeB: 1234,
			},
			Metadata: map[string]string{
				gomdb.CorrelationKey: "demoapp",
			},
		},
		gomdb.NoStreamVersion,
	)
	if err != nil {
		log.Fatalf("writing to stream: %s", err)
	}

	// read from stream
	msg, err := client.GetLastStreamMessage(context.Background(), stream)
	if err != nil {
		log.Fatalf("reading from stream: %s", err)
	}

	// retreive data from message
	data := Message{}
	if err := msg.UnmarshalData(&data); err != nil {
		log.Fatalf("unmarshaling data: %s", err)
	}
	metadata := map[string]string{}
	if err := msg.UnmarshalMetadata(&metadata); err != nil {
		log.Fatalf("unmarshaling metadata: %s", err)
	}

	// print
	log.Println(data)
	log.Println(metadata)
}
