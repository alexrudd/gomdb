package gomdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const (
	// NoStreamVersion is expected version for a stream that doesn't exist.
	NoStreamVersion = int64(-1)
	// AnyVersion allows writing of a message regardless of the stream version.
	AnyVersion = int64(-2)
)

// ErrUnexpectedStreamVersion is returned when a stream is not at the expected
// version when writing a message.
var ErrUnexpectedStreamVersion = errors.New("unexpected stream version when writing message")

// Client exposes the message-db interface.
type Client struct {
	db *sql.DB
}

// NewClient returns a new message-db client for the provided database.
func NewClient(db *sql.DB) *Client {
	return &Client{
		db: db,
	}
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

	// read revision from results.
	var revision int64

	if !rows.Next() {
		return 0, errors.New("write succeeded but no rows were returned")
	}

	if err = rows.Scan(&revision); err != nil {
		return 0, fmt.Errorf("write succeeded but could not read returned revision: %w", err)
	}

	return revision, nil
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

	msgs := make([]*Message, 0, cfg.batchSize) // this may not be efficient
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

	msgs := make([]*Message, 0, cfg.batchSize) // this may not be efficient
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

	// read revision from results.

	if !rows.Next() {
		return 0, errors.New("no rows were returned")
	}

	var value interface{}
	if err = rows.Scan(&value); err != nil {
		return 0, fmt.Errorf("reading stream revision: %w", err)
	}

	if value == nil {
		return NoStreamVersion, nil
	} else if rev, ok := value.(int64); ok {
		return rev, nil
	}

	return 0, fmt.Errorf("unexpected column value type: %T", value)
}
