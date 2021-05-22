package tests

import (
	"context"
	"errors"
	"testing"

	"github.com/alexrudd/gomdb"
	_ "github.com/lib/pq"
)

// TestWriteMessage tests the WriteMessage API.
func TestWriteMessage(t *testing.T) {
	t.Parallel()

	client := NewClient(t)

	t.Run("stream does not exist", func(t *testing.T) {
		t.Parallel()

		stream := NewTestStream(NewTestCategory("new_stream"))
		msg := gomdb.ProposedMessage{
			ID:   GenUUID(),
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

		stream := NewTestStream(NewTestCategory("any_stream"))
		msg := gomdb.ProposedMessage{
			ID:   GenUUID(),
			Type: "TestMessage",
			Data: "data",
		}

		// write initial message
		_, _ = client.WriteMessage(context.TODO(), stream, msg, gomdb.NoStreamVersion)

		// write any version
		msg.ID = GenUUID()
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

		stream := NewTestStream(NewTestCategory("any_stream"))
		msg := gomdb.ProposedMessage{
			ID:   GenUUID(),
			Type: "TestMessage",
			Data: "data",
		}

		// write initial message
		_, _ = client.WriteMessage(context.TODO(), stream, msg, gomdb.NoStreamVersion)

		// write to same version version
		msg.ID = GenUUID()
		_, err := client.WriteMessage(context.TODO(), stream, msg, gomdb.NoStreamVersion)
		if !errors.Is(err, gomdb.ErrUnexpectedStreamVersion) {
			t.Fatal("expected OCC failure")
		}
	})
}
