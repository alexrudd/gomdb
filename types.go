package gomdb

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	// ErrInvalidMessageID is returned when the proposed message ID is not a
	// valid UUID.
	ErrInvalidMessageID = errors.New("proposed message ID must be a valid UUID")
	// ErrMissingType is returned when the proposed message is missing the
	// message type.
	ErrMissingType = errors.New("proposed message must include Type")
	// ErrMissingData is returned when the proposed message is missing any
	// data.
	ErrMissingData = errors.New("proposed message must include Data")
	// ErrMissingCategory is returned when the stream identifier category is
	// missing.
	ErrMissingCategory = errors.New("category cannot be blank")
	// ErrInvalidCategory is returned when the stream identifier category
	// contains the reserved stream name seperator character.
	ErrInvalidCategory = fmt.Errorf("category cannot contain separator (%s)", StreamNameSeparator)
	// ErrMissingStreamID is returned when the stream identifier ID is missing.
	ErrMissingStreamID = errors.New("ID cannot be blank")
)

// StreamNameSeparator is the character used to separate the stream category
// from the stream ID in a stream name.
const StreamNameSeparator = "-"

// Message represents a message that was stored in message-db.
type Message struct {
	ID             string
	Stream         StreamIdentifier
	Type           string
	Version        int64
	GlobalPosition int64
	Timestamp      time.Time
	data           []byte
	metadata       []byte
}

type scanner interface {
	Scan(...interface{}) error
}

func deserialiseMessage(row scanner) (*Message, error) {
	var (
		msg        = &Message{}
		streamName string
	)

	err := row.Scan(&msg.ID, &streamName, &msg.Type, &msg.Version, &msg.GlobalPosition, &msg.data, &msg.metadata, &msg.Timestamp)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	return msg, nil
}

// UnmarshalData attempts to unmarshall the Message's data into the provided
// object.
func (m *Message) UnmarshalData(i interface{}) error {
	return json.Unmarshal(m.data, i)
}

// UnmarshalMetadata attempts to unmarshall the Message's metadata into the
// provided object.
func (m *Message) UnmarshalMetadata(i interface{}) error {
	return json.Unmarshal(m.metadata, i)
}

// ProposedMessage proposes a messages to be written to message-db.
type ProposedMessage struct {
	ID       string
	Type     string
	Data     interface{}
	Metadata interface{}
}

func (pm *ProposedMessage) validate() error {
	if pm.ID == "" {
		return ErrInvalidMessageID
	} else if pm.Type == "" {
		return ErrMissingType
	} else if pm.Data == nil {
		return ErrMissingData
	}

	return nil
}

// StreamIdentifier captures the two components of a message-db stream name.
type StreamIdentifier struct {
	Category string
	ID       string
}

// String returns the string respresentation of a StreamIdentifier.
func (si StreamIdentifier) String() string {
	return si.Category + StreamNameSeparator + si.ID
}

func (si StreamIdentifier) validate() error {
	if si.Category == "" {
		return ErrMissingCategory
	} else if strings.Contains(si.Category, StreamNameSeparator) {
		return ErrInvalidCategory
	} else if si.ID == "" {
		return ErrMissingStreamID
	}

	return nil
}
