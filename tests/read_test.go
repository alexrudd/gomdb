package tests

import (
	"context"
	"testing"

	"github.com/alexrudd/gomdb"
)

// TestGetStreamMessages tests the GetStreamMessages API.
func TestGetStreamMessages(t *testing.T) {
	t.Parallel()

	client := NewClient(t)

	t.Run("stream does not exist", func(t *testing.T) {
		t.Parallel()

		stream := NewTestStream(NewTestCategory("nonexistant"))

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

		stream := NewTestStream(NewTestCategory("entire"))
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

		stream := NewTestStream(NewTestCategory("half"))
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

		stream := NewTestStream(NewTestCategory("half"))
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

		stream := NewTestStream(NewTestCategory("conditional"))
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

		category := PopulateCategory(t, client, NewTestCategory("category"), 5, 10)

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

		category := PopulateCategory(t, client, NewTestCategory("half"), 5, 10)

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

		category := PopulateCategory(t, client, NewTestCategory("batched"), 5, 10)

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

		category := PopulateCategory(t, client, NewTestCategory("consumer"), 5, 10)

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

		category := NewTestCategory("correlation")
		stream := NewTestStream(category)

		// write correlated event
		_, _ = client.WriteMessage(context.TODO(), stream, gomdb.ProposedMessage{
			ID:   GenUUID(),
			Type: "Correlated",
			Data: "data",
			Metadata: map[string]string{
				gomdb.CorrelationKey: "correlated",
			},
		}, gomdb.AnyVersion)

		// write uncorrelated event
		_, _ = client.WriteMessage(context.TODO(), stream, gomdb.ProposedMessage{
			ID:   GenUUID(),
			Type: "Uncorrelated",
			Data: "data",
			Metadata: map[string]string{
				gomdb.CorrelationKey: "uncorrelated",
			},
		}, gomdb.AnyVersion)

		msgs, err := client.GetCategoryMessages(context.TODO(), category, gomdb.WithCorrelation("correlated"))
		if err != nil {
			t.Fatal(err)
		}

		if len(msgs) != 1 {
			t.Fatalf("expected 1 correlated message, actual: %v", len(msgs))
		}

		if msgs[0].Type != "Correlated" {
			t.Fatalf("expected message type of Correlated, actual: %s", msgs[0].Type)
		}

	})

	t.Run("read with condition", func(t *testing.T) {
		t.Parallel()

		if !*isConditionOn {
			t.Skip()
		}

		category := PopulateCategory(t, client, NewTestCategory("condition"), 5, 10)

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

		stream := NewTestStream(NewTestCategory("nonexistant"))

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

		stream := NewTestStream(NewTestCategory("stream"))
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

		stream := NewTestStream(NewTestCategory("nonexistant"))

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

		stream := NewTestStream(NewTestCategory("stream"))
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
