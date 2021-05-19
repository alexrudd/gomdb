// Package gomdb provides a Client for calling Message DB procedures.
package gomdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	// NoStreamVersion is expected version for a stream that doesn't exist.
	NoStreamVersion = int64(-1)
	// AnyVersion allows writing of a message regardless of the stream version.
	AnyVersion = int64(-2)
	// DefaultPollingInterval defines the default polling duration for
	// subscriptions.
	DefaultPollingInterval = 100 * time.Millisecond
)

// ErrUnexpectedStreamVersion is returned when a stream is not at the expected
// version when writing a message.
var ErrUnexpectedStreamVersion = errors.New("unexpected stream version when writing message")

// Client exposes the message-db interface.
type Client struct {
	db              *sql.DB
	pollingStrategy PollingStrategy
}

// NewClient returns a new message-db client for the provided database.
func NewClient(db *sql.DB, opts ...ClientOption) *Client {
	c := &Client{
		db:              db,
		pollingStrategy: ConstantPolling(DefaultPollingInterval),
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

// WriteMessage attempted to write the proposed message to the specifed stream.
func (c *Client) WriteMessage(ctx context.Context, stream StreamIdentifier, message ProposedMessage, expectedVersion int64) (int64, error) {
	// validate inputs
	if err := stream.validate(); err != nil {
		return 0, fmt.Errorf("validating stream identifier: %w", err)
	} else if err := message.validate(); err != nil {
		return 0, fmt.Errorf("validating message: %w", err)
	}

	// Marshal data and metadata.
	data, err := json.Marshal(message.Data)
	if err != nil {
		return 0, fmt.Errorf("marshaling data to json: %w", err)
	}

	metadata, err := json.Marshal(message.Metadata)
	if err != nil {
		return 0, fmt.Errorf("marshaling metadata to json: %w", err)
	}

	// set expected version to nil to skip OCC check.
	ev := interface{}(expectedVersion)
	if expectedVersion == AnyVersion {
		ev = nil
	}

	// prepare and execute query.
	stmt, err := c.db.PrepareContext(ctx, WriteMessageSQL)
	if err != nil {
		return 0, fmt.Errorf("preparing write statement: %w", err)
	}

	rows, err := stmt.QueryContext(ctx, message.ID, stream.String(), message.Type, data, metadata, ev)
	if err != nil {
		if strings.Contains(err.Error(), "Wrong expected version") {
			return 0, ErrUnexpectedStreamVersion
		}
		return 0, fmt.Errorf("executing write statement: %w", err)
	}

	defer rows.Close()

	// read version from results.
	var version int64

	if !rows.Next() {
		return 0, errors.New("write succeeded but no rows were returned")
	}

	if err = rows.Scan(&version); err != nil {
		return 0, fmt.Errorf("write succeeded but could not read returned version: %w", err)
	}

	return version, nil
}

// GetStreamMessages reads messages from an individual stream. By default the
// stream is read from the beginning with a batch size of 1000. Use
// GetStreamOptions to adjust this behaviour.
func (c *Client) GetStreamMessages(ctx context.Context, stream StreamIdentifier, opts ...GetStreamOption) ([]*Message, error) {
	cfg := newDefaultStreamConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	// validate inputs
	if err := stream.validate(); err != nil {
		return nil, fmt.Errorf("validating stream identifier: %w", err)
	} else if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("validating options: %w", err)
	}

	// prepare and execute query.
	stmt, err := c.db.PrepareContext(ctx, GetStreamMessagesSQL)
	if err != nil {
		return nil, fmt.Errorf("preparing get stream statement: %w", err)
	}

	rows, err := stmt.QueryContext(ctx, stream.String(), cfg.version, cfg.batchSize, cfg.getCondition())
	if err != nil {
		return nil, fmt.Errorf("executing get stream statement: %w", err)
	}

	defer rows.Close()

	msgs := []*Message{}
	for rows.Next() {
		msg, err := deserialiseMessage(rows)
		if err != nil {
			return msgs, fmt.Errorf("deserialising message: %w", err)
		} else if msg == nil {
			continue
		}

		msgs = append(msgs, msg)
	}

	return msgs, nil
}

// GetCategoryMessages reads messages from a category. By default the category
// is read from the beginning of the message store with a batch size of 1000.
// Use GetCategoryOptions to adjust this behaviour and to configure consumer
// groups and filtering.
func (c *Client) GetCategoryMessages(ctx context.Context, category string, opts ...GetCategoryOption) ([]*Message, error) {
	cfg := newDefaultCategoryConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	// validate inputs
	if strings.Contains(category, StreamNameSeparator) {
		return nil, fmt.Errorf("category cannot contain stream name separator (%s)", StreamNameSeparator)
	} else if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("validating options: %w", err)
	}

	// prepare and execute query.
	stmt, err := c.db.PrepareContext(ctx, GetCategoryMessagesSQL)
	if err != nil {
		return nil, fmt.Errorf("preparing get stream statement: %w", err)
	}

	rows, err := stmt.QueryContext(ctx, category, cfg.position, cfg.batchSize, cfg.getCorrelation(), cfg.getConsumerGroupMember(), cfg.getConsumerGroupSize(), cfg.getCondition())
	if err != nil {
		return nil, fmt.Errorf("executing get stream statement: %w", err)
	}

	defer rows.Close()

	msgs := []*Message{}
	for rows.Next() {
		msg, err := deserialiseMessage(rows)
		if err != nil {
			return msgs, err
		} else if msg == nil {
			continue
		}

		msgs = append(msgs, msg)
	}

	return msgs, nil
}

// GetLastStreamMessage returns the last message for the specified stream, or
// nil if the stream is empty.
func (c *Client) GetLastStreamMessage(ctx context.Context, stream StreamIdentifier) (*Message, error) {
	// validate inputs
	if err := stream.validate(); err != nil {
		return nil, fmt.Errorf("validating stream identifier: %w", err)
	}

	// prepare and execute query.
	stmt, err := c.db.PrepareContext(ctx, GetLastStreamMessageSQL)
	if err != nil {
		return nil, fmt.Errorf("preparing get stream statement: %w", err)
	}

	rows, err := stmt.QueryContext(ctx, stream.String())
	if err != nil {
		return nil, fmt.Errorf("executing get stream statement: %w", err)
	}

	defer rows.Close()

	if !rows.Next() {
		return nil, nil
	}

	msg, err := deserialiseMessage(rows)
	if err != nil {
		return nil, fmt.Errorf("deserialising message: %w", err)
	}

	return msg, nil
}

// GetStreamVersion returns the version of the specified stream. Always check
// the error value before using the returned version.
func (c *Client) GetStreamVersion(ctx context.Context, stream StreamIdentifier) (int64, error) {
	// validate inputs
	if err := stream.validate(); err != nil {
		return 0, fmt.Errorf("validating stream identifier: %w", err)
	}

	// prepare and execute query.
	stmt, err := c.db.PrepareContext(ctx, GetStreamVersionSQL)
	if err != nil {
		return 0, fmt.Errorf("preparing get stream version statement: %w", err)
	}

	rows, err := stmt.QueryContext(ctx, stream.String())
	if err != nil {
		return 0, fmt.Errorf("executing get stream version statement: %w", err)
	}

	defer rows.Close()

	// read version from results.

	if !rows.Next() {
		return 0, errors.New("no rows were returned")
	}

	var value interface{}
	if err = rows.Scan(&value); err != nil {
		return 0, fmt.Errorf("reading stream version: %w", err)
	}

	if value == nil {
		return NoStreamVersion, nil
	} else if rev, ok := value.(int64); ok {
		return rev, nil
	}

	return 0, fmt.Errorf("unexpected column value type: %T", value)
}

// MessageHandler handles messages as they appear after being written.
type MessageHandler func(*Message)

// LivenessHandler handles whether the subscription is in a "live" state or
// whether it is catching up.
type LivenessHandler func(bool)

// SubDroppedHandler handles errors that appear and stop the subscription.
type SubDroppedHandler func(error)

// SubscribeToStream subscribes to a stream and asynchronously passes messages
// to the message handler in batches. Once a subscription has caught up it will
// poll the database periodically for new messages. To stop a subscription
// cancel the provided context.
// When a subscription catches up it will call the LivenessHandler with true. If
// the subscription falls behind again it will called the LivenessHandler with
// false.
// If there is an error while reading messages then the subscription will be
// stopped and the SubDroppedHandler will be called with the stopping error. If
// the subscription is cancelled then the SubDroppedHandler will be called with
// nil.
func (c *Client) SubscribeToStream(
	ctx context.Context,
	stream StreamIdentifier,
	handleMessage MessageHandler,
	handleLiveness LivenessHandler,
	handleDropped SubDroppedHandler,
	opts ...GetStreamOption,
) error {
	cfg := newDefaultStreamConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	// validate inputs
	if err := stream.validate(); err != nil {
		return fmt.Errorf("validating stream identifier: %w", err)
	} else if handleMessage == nil || handleLiveness == nil || handleDropped == nil {
		return errors.New("all subscription handlers are required")
	} else if err := cfg.validate(); err != nil {
		return fmt.Errorf("validating options: %w", err)
	}

	// ignore context cancelled errors
	wrappedHandleDropped := func(e error) {
		if errors.Is(e, context.Canceled) {
			handleDropped(nil)
		} else {
			handleDropped(ctx.Err())
		}
	}

	go func() {
		poll := time.NewTimer(0)
		live := false
		defer poll.Stop()

		for {
			// check for context cancelled
			select {
			case <-ctx.Done():
				wrappedHandleDropped(ctx.Err())
				return
			case <-poll.C:
			}

			msgs, err := c.GetStreamMessages(ctx, stream, func(c *streamConfig) { *c = *cfg })
			if err != nil {
				wrappedHandleDropped(err)
				return
			}

			for _, msg := range msgs {
				handleMessage(msg)
			}

			if len(msgs) > 0 {
				cfg.version = msgs[len(msgs)-1].Version + 1
			}

			// if we've read fewer messages than the batch size we must have
			// caught up and can go live. Otherwise we've fallen behind.
			if len(msgs) < int(cfg.batchSize) && !live {
				live = true
				handleLiveness(live)
			} else if len(msgs) == int(cfg.batchSize) && live {
				live = false
				handleLiveness(live)
			}

			poll.Reset(c.pollingStrategy(int64(len(msgs)), cfg.batchSize))
		}
	}()

	return nil
}

// SubscribeToCategory subscribes to a category and asynchronously passes messages
// to the message handler in batches. Once a subscription has caught up it will
// poll the database periodically for new messages. To stop a subscription
// cancel the provided context.
// When a subscription catches up it will call the LivenessHandler with true. If
// the subscription falls behind again it will called the LivenessHandler with
// false.
// If there is an error while reading messages then the subscription will be
// stopped and the SubDroppedHandler will be called with the stopping error. If
// the subscription is cancelled then the SubDroppedHandler will be called with
// nil.
func (c *Client) SubscribeToCategory(
	ctx context.Context,
	category string,
	handleMessage MessageHandler,
	handleLiveness LivenessHandler,
	handleDropped SubDroppedHandler,
	opts ...GetCategoryOption,
) error {
	cfg := newDefaultCategoryConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	// validate inputs
	if strings.Contains(category, StreamNameSeparator) {
		return fmt.Errorf("category cannot contain stream name separator (%s)", StreamNameSeparator)
	} else if handleMessage == nil || handleLiveness == nil || handleDropped == nil {
		return errors.New("all subscription handlers are required")
	} else if err := cfg.validate(); err != nil {
		return fmt.Errorf("validating options: %w", err)
	}

	// ignore context cancelled errors
	wrappedHandleDropped := func(e error) {
		if errors.Is(e, context.Canceled) {
			handleDropped(nil)
		} else {
			handleDropped(ctx.Err())
		}
	}

	go func() {
		poll := time.NewTimer(0)
		live := false
		defer poll.Stop()

		for {
			// check for context cancelled
			select {
			case <-ctx.Done():
				wrappedHandleDropped(ctx.Err())
				return
			case <-poll.C:
			}

			msgs, err := c.GetCategoryMessages(ctx, category, func(c *categoryConfig) { *c = *cfg })
			if err != nil {
				wrappedHandleDropped(err)
				return
			}

			for _, msg := range msgs {
				handleMessage(msg)
			}

			if len(msgs) > 0 {
				cfg.position = msgs[len(msgs)-1].GlobalPosition + 1
			}

			// if we've read fewer messages than the batch size we must have
			// caught up and can go live. Otherwise we've fallen behind.
			if len(msgs) < int(cfg.batchSize) && !live {
				live = true
				handleLiveness(live)
			} else if len(msgs) == int(cfg.batchSize) && live {
				live = false
				handleLiveness(live)
			}

			poll.Reset(c.pollingStrategy(int64(len(msgs)), cfg.batchSize))
		}
	}()

	return nil
}
