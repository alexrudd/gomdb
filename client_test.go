package gomdb_test

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"testing"

	"github.com/alexrudd/gomdb"
	"github.com/gofrs/uuid"
	_ "github.com/lib/pq"
	"github.com/thanhpk/randstr"
)

var (
	host          = flag.String("host", "localhost", "the test db host")
	port          = flag.Int("port", 5432, "the test db port")
	dbname        = flag.String("dbname", "message_store", "the message-db database")
	user          = flag.String("user", "message_store", "the user to connect as")
	password      = flag.String("password", "", "the password to use to login")
	sslmode       = flag.String("sslmode", "disable", "the ssl mode to connect with")
	isConditionOn = flag.Bool("condition-on", false, "is the SQL condition feature on")
)

func Init() {
	flag.Parse()
}

// NewClient opens a new DB connection then creates and returns a Client.
func NewClient(t *testing.T) *gomdb.Client {
	t.Helper()

	conn := fmt.Sprintf("host=%s port=%v dbname=%s user=%s sslmode=%s",
		*host, *port, *dbname, *user, *sslmode)

	if *password != "" {
		conn += " password=" + *password
	}

	db, err := sql.Open("postgres", conn)
	if err != nil {
		t.Fatalf("opening db (%s): %s", conn, err)
	}

	t.Cleanup(func() {
		db.Close()
	})

	// _, err = db.Exec("SET message_store.sql_condition TO on;")
	// if err != nil {
	// 	t.Fatalf("enabling condition parameter: %s", err)
	// }

	_, err = db.Exec("SET search_path TO message_store,public;")
	if err != nil {
		t.Fatalf("setting search path: %s", err)
	}

	return gomdb.NewClient(db)
}

// NewTestStream creates a new StreamIdentifier using the provided category
// prefix.
func NewTestStream(catPrefix string) gomdb.StreamIdentifier {
	randstr.Base62(5)

	return gomdb.StreamIdentifier{
		Category: catPrefix + randstr.Base62(5),
		ID:       randstr.Base62(10),
	}
}

// PopulateStream creates the specified number of messages and writes them
// to the specified stream.
func PopulateStream(t *testing.T, client *gomdb.Client, stream gomdb.StreamIdentifier, messages int) {
	t.Helper()

	var (
		version = gomdb.NoStreamVersion
		err     error
	)

	for i := 0; i < messages; i++ {
		version, err = client.WriteMessage(context.TODO(), stream, gomdb.ProposedMessage{
			ID:   uuid.NewV4().String(),
			Type: "TestMessage",
			Data: "data",
		}, version)
		if err != nil {
			t.Fatal(err)
		}
	}
}

// PopulateCategory creates multiple streams within a single categatory and
// populates them with messages. The actual category is returned.
func PopulateCategory(t *testing.T, client *gomdb.Client, catPrefix string, streams, messages int) string {
	t.Helper()

	category := catPrefix + randstr.Base62(5)

	for i := 0; i < streams; i++ {
		stream := gomdb.StreamIdentifier{
			Category: category,
			ID:       randstr.Base62(10),
		}

		PopulateStream(t, client, stream, messages)
	}

	return category
}

// TestWriteMessage tests the WriteMessage API.
func TestWriteMessage(t *testing.T) {
	t.Parallel()

	client := NewClient(t)

	t.Run("stream does not exist", func(t *testing.T) {
		t.Parallel()

		stream := NewTestStream("new_stream")
		msg := gomdb.ProposedMessage{
			ID:   uuid.NewV4().String(),
			Type: "TestMessage",
			Data: "data",
		}

		version, err := client.WriteMessage(context.TODO(), stream, msg, gomdb.NoStreamVersion)
		if err != nil {
			t.Fatal(err)
		}

		if version != 0 {
			t.Fatalf("expected ver 0, got version %v", version)
		}
	})

	t.Run("skip OCC check", func(t *testing.T) {
		t.Parallel()

		stream := NewTestStream("any_stream")
		msg := gomdb.ProposedMessage{
			ID:   uuid.NewV4().String(),
			Type: "TestMessage",
			Data: "data",
		}

		// write initial message
		_, _ = client.WriteMessage(context.TODO(), stream, msg, gomdb.NoStreamVersion)

		// write any version
		msg.ID = uuid.NewV4().String()
		version, err := client.WriteMessage(context.TODO(), stream, msg, gomdb.AnyVersion)
		if err != nil {
			t.Fatal(err)
		}

		if version != 1 {
			t.Fatalf("expected ver 1, got version %v", version)
		}
	})

	t.Run("fail OCC check", func(t *testing.T) {
		t.Parallel()

		stream := NewTestStream("any_stream")
		msg := gomdb.ProposedMessage{
			ID:   uuid.NewV4().String(),
			Type: "TestMessage",
			Data: "data",
		}

		// write initial message
		_, _ = client.WriteMessage(context.TODO(), stream, msg, gomdb.NoStreamVersion)

		// write to same version version
		msg.ID = uuid.NewV4().String()
		_, err := client.WriteMessage(context.TODO(), stream, msg, gomdb.NoStreamVersion)
		if !errors.Is(err, gomdb.ErrUnexpectedStreamVersion) {
			t.Fatal("expected OCC failure")
		}
	})
}

// TestGetStreamMessages tests the GetStreamMessages API.
func TestGetStreamMessages(t *testing.T) {
	t.Parallel()

	client := NewClient(t)

	t.Run("stream does not exist", func(t *testing.T) {
		t.Parallel()

		stream := NewTestStream("nonexistant")

		msgs, err := client.GetStreamMessages(context.TODO(), stream)
		if err != nil {
			t.Fatal(err)
		}

		if len(msgs) != 0 {
			t.Fatalf("expected no messages, got %v", len(msgs))
		}
	})

	t.Run("get entire stream", func(t *testing.T) {
		t.Parallel()

		stream := NewTestStream("entire")
		PopulateStream(t, client, stream, 10)

		msgs, err := client.GetStreamMessages(context.TODO(), stream)
		if err != nil {
			t.Fatal(err)
		}

		if len(msgs) != 10 {
			t.Fatalf("expected 10 messages, got %v", len(msgs))
		}
	})

	t.Run("get first half of stream", func(t *testing.T) {
		t.Parallel()

		stream := NewTestStream("half")
		PopulateStream(t, client, stream, 10)

		msgs, err := client.GetStreamMessages(context.TODO(), stream, gomdb.WithStreamBatchSize(5))
		if err != nil {
			t.Fatal(err)
		}

		if len(msgs) != 5 {
			t.Fatalf("expected 5 messages, got %v", len(msgs))
		}

		for ver, msg := range msgs {
			if msg.Version != int64(ver) {
				t.Fatalf("expected first message with version %v, got %v", ver, msg.Version)
			}
		}
	})

	t.Run("get second half of stream", func(t *testing.T) {
		t.Parallel()

		stream := NewTestStream("half")
		PopulateStream(t, client, stream, 10)

		msgs, err := client.GetStreamMessages(context.TODO(), stream, gomdb.WithStreamBatchSize(5), gomdb.FromVersion(5))
		if err != nil {
			t.Fatal(err)
		}

		if len(msgs) != 5 {
			t.Fatalf("expected 10 messages, got %v", len(msgs))
		}

		for ver, msg := range msgs {
			if msg.Version != int64(ver+5) {
				t.Fatalf("expected first message with version %v, got %v", ver+5, msg.Version)
			}
		}
	})

	t.Run("get stream with condition", func(t *testing.T) {
		t.Parallel()

		if !*isConditionOn {
			t.Skip()
		}

		stream := NewTestStream("conditional")
		PopulateStream(t, client, stream, 10)

		msgs, err := client.GetStreamMessages(context.TODO(), stream,
			gomdb.WithStreamCondition("MOD(messages.position, 2) = 0"),
		)
		if err != nil {
			t.Fatal(err)
		}

		if len(msgs) != 5 {
			t.Fatalf("expected 10 messages, got %v", len(msgs))
		}

		for _, msg := range msgs {
			if msg.Version%2 != 0 {
				t.Fatalf("expected only even versions, got %v", msg.Version)
			}
		}
	})
}

// TestGetCategoryMessages tests the GetCategoryMessages API.
func TestGetCategoryMessages(t *testing.T) {
	t.Parallel()

	client := NewClient(t)

	t.Run("stream does not exist", func(t *testing.T) {
		t.Parallel()

		msgs, err := client.GetCategoryMessages(context.TODO(), "nonexistant")
		if err != nil {
			t.Fatal(err)
		}

		if len(msgs) != 0 {
			t.Fatalf("expected no messages, got %v", len(msgs))
		}
	})

	t.Run("get all messages for category", func(t *testing.T) {
		t.Parallel()

		category := PopulateCategory(t, client, "category", 5, 10)

		msgs, err := client.GetCategoryMessages(context.TODO(), category)
		if err != nil {
			t.Fatal(err)
		}

		if len(msgs) != 50 {
			t.Fatalf("expected 50 messages, got %v", len(msgs))
		}
	})

	t.Run("get half messages for category", func(t *testing.T) {
		t.Parallel()

		category := PopulateCategory(t, client, "half", 5, 10)

		// read all messages.
		msgs, _ := client.GetCategoryMessages(context.TODO(), category)
		middlePos := msgs[25].GlobalPosition

		// re-read half messages and compare
		halfMsgs, err := client.GetCategoryMessages(context.TODO(), category, gomdb.FromPosition(middlePos))
		if err != nil {
			t.Fatal(err)
		}

		for i, msg := range halfMsgs {
			if msg.GlobalPosition != msgs[25+i].GlobalPosition {
				t.Fatalf("expected position %v messages, got %v", msgs[25+i].GlobalPosition, msg.GlobalPosition)
			}
		}
	})

	t.Run("read only first 15 messages in category", func(t *testing.T) {
		t.Parallel()

		category := PopulateCategory(t, client, "batched", 5, 10)

		msgs, err := client.GetCategoryMessages(context.TODO(), category, gomdb.WithCategoryBatchSize(15))
		if err != nil {
			t.Fatal(err)
		}

		if len(msgs) != 15 {
			t.Fatalf("expected 15 messages, got %v", len(msgs))
		}
	})

	t.Run("read as consumer group", func(t *testing.T) {
		t.Parallel()

		category := PopulateCategory(t, client, "batched", 5, 10)

		msgs1, err := client.GetCategoryMessages(context.TODO(), category, gomdb.AsConsumerGroup(0, 2))
		if err != nil {
			t.Fatal(err)
		}

		msgs2, err := client.GetCategoryMessages(context.TODO(), category, gomdb.AsConsumerGroup(1, 2))
		if err != nil {
			t.Fatal(err)
		}

		msgs := append(msgs1, msgs2...)

		if len(msgs) != 50 {
			t.Fatalf("expected 50 messages, got %v", len(msgs))
		}
	})

	t.Run("read with correlation", func(t *testing.T) {
		t.Parallel()

		t.Skip() // TODO
	})

	t.Run("read with condition", func(t *testing.T) {
		t.Parallel()

		if !*isConditionOn {
			t.Skip()
		}

		category := PopulateCategory(t, client, "batched", 5, 10)

		msgs, err := client.GetCategoryMessages(context.TODO(), category,
			gomdb.WithCategoryCondition("MOD(messages.position, 2) = 0"),
		)
		if err != nil {
			t.Fatal(err)
		}

		if len(msgs) != 25 {
			t.Fatalf("expected 10 messages, got %v", len(msgs))
		}

		for _, msg := range msgs {
			if msg.Version%2 != 0 {
				t.Fatalf("expected only even versions, got %v", msg.Version)
			}
		}
	})
}

// TestGetLastStreamMessage tests the GetLastStreamMessage API.
func TestGetLastStreamMessage(t *testing.T) {
	t.Parallel()

	client := NewClient(t)

	t.Run("stream does not exist", func(t *testing.T) {
		t.Parallel()

		stream := NewTestStream("nonexistant")

		msg, err := client.GetLastStreamMessage(context.TODO(), stream)
		if err != nil {
			t.Fatal(err)
		}

		if msg != nil {
			t.Fatalf("expected no message, got %v", msg)
		}
	})

	t.Run("get last message", func(t *testing.T) {
		t.Parallel()

		stream := NewTestStream("stream")
		PopulateStream(t, client, stream, 3)

		msg, err := client.GetLastStreamMessage(context.TODO(), stream)
		if err != nil {
			t.Fatal(err)
		}

		if msg == nil {
			t.Fatal("expected message but got nil")
		} else if msg.Version != 2 {
			t.Fatalf("expected message with version %v, actual %v", 2, msg.Version)
		}
	})
}

// TestGetStreamVersion tests the GetStreamVersion API.
func TestGetStreamVersion(t *testing.T) {
	t.Parallel()

	client := NewClient(t)

	t.Run("stream does not exist", func(t *testing.T) {
		t.Parallel()

		stream := NewTestStream("nonexistant")

		version, err := client.GetStreamVersion(context.TODO(), stream)
		if err != nil {
			t.Fatal(err)
		}

		if version != -1 {
			t.Fatalf("expected version -1, actual %v", version)
		}
	})

	t.Run("get stream version", func(t *testing.T) {
		t.Parallel()

		stream := NewTestStream("stream")
		PopulateStream(t, client, stream, 3)

		version, err := client.GetStreamVersion(context.TODO(), stream)
		if err != nil {
			t.Fatal(err)
		}

		if version != 2 {
			t.Fatalf("expected stream version %v, actual %v", 2, version)
		}
	})
}
