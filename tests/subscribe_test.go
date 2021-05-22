package tests

import (
	"context"
	"sync"
	"testing"

	"github.com/alexrudd/gomdb"
)

// TestSubscribeToStream tests the SubscribeToStream API.
func TestSubscribeToStream(t *testing.T) {
	t.Parallel()

	client := NewClient(t)

	t.Run("subscribe to empty stream", func(t *testing.T) {
		t.Parallel()

		stream := NewTestStream(NewTestCategory("empty"))
		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()

		goneLive := sync.WaitGroup{}
		goneLive.Add(1)

		err := client.SubscribeToStream(
			ctx,
			stream,
			func(m *gomdb.Message) {
				t.Fatal("No messages should exist on stream")
			},
			func(live bool) {
				if !live {
					t.Fatal("subscription should be live")
				}
				goneLive.Done()
			},
			func(err error) {
				if err != nil {
					t.Fatalf("received subscription error: %s", err)
				}
			},
		)
		if err != nil {
			t.Fatal(err)
		}

		goneLive.Wait()
	})

	t.Run("subscribe to new messages", func(t *testing.T) {
		t.Parallel()

		stream := NewTestStream(NewTestCategory("incoming"))
		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()

		received := sync.WaitGroup{}
		received.Add(3)

		err := client.SubscribeToStream(
			ctx,
			stream,
			func(m *gomdb.Message) {
				received.Done()
			},
			func(live bool) {},
			func(err error) {},
		)
		if err != nil {
			t.Fatal(err)
		}

		PopulateStream(t, client, stream, 3)

		received.Wait()
	})

	t.Run("catch up to stream then go live", func(t *testing.T) {
		t.Parallel()

		stream := NewTestStream(NewTestCategory("catchup"))
		PopulateStream(t, client, stream, 10)

		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()

		received := sync.WaitGroup{}
		received.Add(10)
		version := int64(0)

		err := client.SubscribeToStream(
			ctx,
			stream,
			func(m *gomdb.Message) {
				version = m.Version
				received.Done()
			},
			func(live bool) {
				if live && version != 9 {
					t.Fatalf("expected to go live at version 9, actual: %v", version)
				}
			},
			func(err error) {},
			gomdb.WithStreamBatchSize(5),
		)
		if err != nil {
			t.Fatal(err)
		}

		received.Wait()
	})
}

// TestSubscribeToCategory tests the SubscribeToCategory API.
func TestSubscribeToCategory(t *testing.T) {
	t.Parallel()

	client := NewClient(t)

	t.Run("subscribe to empty category", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()

		goneLive := sync.WaitGroup{}
		goneLive.Add(1)

		err := client.SubscribeToCategory(
			ctx,
			NewTestCategory("empty"),
			func(m *gomdb.Message) {
				t.Fatal("No messages should exist on stream")
			},
			func(live bool) {
				if !live {
					t.Fatal("subscription should be live")
				}
				goneLive.Done()
			},
			func(err error) {
				if err != nil {
					t.Fatalf("received subscription error: %s", err)
				}
			},
		)
		if err != nil {
			t.Fatal(err)
		}

		goneLive.Wait()
	})

	t.Run("subscribe to new category messages", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()

		goneLive := sync.WaitGroup{}
		goneLive.Add(1)
		received := sync.WaitGroup{}
		received.Add(30)

		category := NewTestCategory("empty")

		err := client.SubscribeToCategory(
			ctx,
			category,
			func(m *gomdb.Message) {
				received.Done()
			},
			func(live bool) {
				if !live {
					t.Fatal("subscription should be live")
				}
				goneLive.Done()
			},
			func(err error) {
				if err != nil {
					t.Fatalf("received subscription error: %s", err)
				}
			},
		)
		if err != nil {
			t.Fatal(err)
		}

		goneLive.Wait()

		PopulateCategory(t, client, category, 3, 10)

		received.Wait()
	})

	t.Run("catch up to category then go live", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()

		goneLive := sync.WaitGroup{}
		goneLive.Add(1)
		received := sync.WaitGroup{}
		received.Add(30)

		category := NewTestCategory("empty")
		PopulateCategory(t, client, category, 3, 10)

		err := client.SubscribeToCategory(
			ctx,
			category,
			func(m *gomdb.Message) {
				received.Done()
			},
			func(live bool) {
				if !live {
					t.Fatal("subscription should be live")
				}
				goneLive.Done()
			},
			func(err error) {
				if err != nil {
					t.Fatalf("received subscription error: %s", err)
				}
			},
		)
		if err != nil {
			t.Fatal(err)
		}

		received.Wait()
		goneLive.Wait()

		// receive 10 more messages live
		received.Add(10)
		PopulateCategory(t, client, category, 2, 5)
		received.Wait()
	})
}
