package gomdb

import (
	"encoding/json"
	"errors"
	"testing"
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
				ID:   "someID",
				Data: "data",
			},
			expErr: ErrMissingType,
		},
		{
			name: "missing data",
			message: ProposedMessage{
				ID:   "someID",
				Type: "SomeType",
			},
			expErr: ErrMissingData,
		},
		{
			name: "valid",
			message: ProposedMessage{
				ID:   "someID",
				Type: "SomeType",
				Data: "data",
			},
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

func Test_ParseStreamIdentifier(t *testing.T) {
	testcases := []struct {
		input  string
		sid    StreamIdentifier
		expErr error
	}{
		{
			input:  "",
			sid:    StreamIdentifier{},
			expErr: ErrInvalidStreamIdentifier,
		},
		{
			input:  "-",
			sid:    StreamIdentifier{},
			expErr: ErrInvalidStreamIdentifier,
		},
		{
			input: "a-b",
			sid: StreamIdentifier{
				Category: "a",
				ID:       "b",
			},
		},
		{
			input:  "cat",
			sid:    StreamIdentifier{},
			expErr: ErrInvalidStreamIdentifier,
		},
		{
			input:  "cat_egory",
			sid:    StreamIdentifier{},
			expErr: ErrInvalidStreamIdentifier,
		},
		{
			input: "cat_egory-123abc",
			sid: StreamIdentifier{
				Category: "cat_egory",
				ID:       "123abc",
			},
		},
		{
			input: "cat_egory-123-abc",
			sid: StreamIdentifier{
				Category: "cat_egory",
				ID:       "123-abc",
			},
		},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.input, func(t *testing.T) {
			sid, err := ParseStreamIdentifier(tc.input)

			if sid.Category != tc.sid.Category {
				t.Errorf("category: expected %s, actual %s", tc.sid.Category, sid.Category)
			}
			if sid.ID != tc.sid.ID {
				t.Errorf("ID: expected %s, actual %s", tc.sid.ID, sid.ID)
			}
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
			name: "valid",
			sid: StreamIdentifier{
				Category: "category",
				ID:       "123-abc",
			},
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
