package gomdb

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/gofrs/uuid"
)

func Test_ProposedMessage_validate(t *testing.T) {
	testcases := []struct {
		name    string
		message ProposedMessage
		expErr  error
	}{
		{
			name: "missing UUID",
			message: ProposedMessage{
				Type: "SomeType",
				Data: "data",
			},
			expErr: ErrInvalidMessageID,
		},
		{
			name: "missing type",
			message: ProposedMessage{
				ID:   uuid.NewV4().String(),
				Data: "data",
			},
			expErr: ErrMissingType,
		},
		{
			name: "missing data",
			message: ProposedMessage{
				ID:   uuid.NewV4().String(),
				Type: "SomeType",
			},
			expErr: ErrMissingData,
		},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			err := tc.message.validate()
			if !errors.Is(err, tc.expErr) {
				t.Fatalf("expected %v, actual %v", tc.expErr, err)
			}
		})
	}
}

func Test_StreamIdentifier_validate(t *testing.T) {
	testcases := []struct {
		name   string
		sid    StreamIdentifier
		expErr error
	}{
		{
			name: "missing category",
			sid: StreamIdentifier{
				ID: "123abc",
			},
			expErr: ErrMissingCategory,
		},
		{
			name: "invalid category",
			sid: StreamIdentifier{
				Category: "cat-egory",
				ID:       "123abc",
			},
			expErr: ErrInvalidCategory,
		},
		{
			name: "missing ID",
			sid: StreamIdentifier{
				Category: "category",
			},
			expErr: ErrMissingStreamID,
		},
		{
			name: "invalid ID",
			sid: StreamIdentifier{
				Category: "category",
				ID:       "123-abc",
			},
			expErr: ErrInvalidStreamID,
		},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			err := tc.sid.validate()
			if !errors.Is(err, tc.expErr) {
				t.Fatalf("expected %v, actual %v", tc.expErr, err)
			}
		})
	}
}

func Test_Message_Unmarshal(t *testing.T) {
	data := "some data"
	metadata := "some metadata"

	dataJson, _ := json.Marshal(data)
	metadataJson, _ := json.Marshal(metadata)

	msg := Message{
		data:     dataJson,
		metadata: metadataJson,
	}

	var (
		outData     string
		outMetadata string
	)

	err := msg.UnmarshalData(&outData)
	if err != nil {
		t.Fatalf("unexpected error unmarshaling data: %s", err)
	} else if outData != data {
		t.Fatalf("expected %s, actual %s", data, outData)
	}

	err = msg.UnmarshalMetadata(&outMetadata)
	if err != nil {
		t.Fatalf("unexpected error unmarshaling metadata: %s", err)
	} else if outMetadata != metadata {
		t.Fatalf("expected %s, actual %s", metadata, outMetadata)
	}
}
