package gomdb

import (
	"errors"
	"testing"
	"time"
)

func Test_PollingStrategies(t *testing.T) {
	t.Parallel()

	testcases := []struct {
		name      string
		strategy  PollingStrategy
		retrieved []int64
		expected  int64
		delays    []time.Duration
	}{
		{
			name:      "constant polling",
			strategy:  ConstantPolling(time.Second)(),
			retrieved: []int64{0, 1, 2},
			expected:  2,
			delays:    []time.Duration{time.Second, time.Second, 0},
		},
		{
			name:      "exponential polling",
			strategy:  ExpBackoffPolling(time.Second, 10*time.Second, 2)(),
			retrieved: []int64{2, 1, 0, 0, 0, 0, 0, 0},
			expected:  2,
			delays: []time.Duration{
				0,
				time.Second,
				time.Second,
				2 * time.Second,
				4 * time.Second,
				8 * time.Second,
				10 * time.Second,
				10 * time.Second,
			},
		},
		{
			name:      "dynamic polling",
			strategy:  DynamicPolling(0.75, 50*time.Millisecond, 100*time.Millisecond, time.Second)(),
			retrieved: []int64{90, 50, 60, 70, 80, 75, 100},
			expected:  100,
			delays: []time.Duration{
				100 * time.Millisecond,
				150 * time.Millisecond,
				200 * time.Millisecond,
				250 * time.Millisecond,
				200 * time.Millisecond,
				200 * time.Millisecond,
				0,
			},
		},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			for i, r := range tc.retrieved {
				d := tc.strategy(r, tc.expected)
				if d != tc.delays[i] {
					t.Fatalf("on retreived %v/%v expected delay %s, actual %s", r, tc.expected, tc.delays[i], d)
				}
			}
		})
	}
}

func Test_categoryConfig_validate(t *testing.T) {
	testcases := []struct {
		name   string
		config categoryConfig
		expErr error
	}{
		{
			name: "invalid stream position",
			config: categoryConfig{
				position:  -1,
				batchSize: 1,
			},
			expErr: ErrInvalidReadPosition,
		},
		{
			name: "invalid batch size",
			config: categoryConfig{
				position:  0,
				batchSize: 0,
			},
			expErr: ErrInvalidReadBatchSize,
		},
		{
			name: "negative consumer member index",
			config: categoryConfig{
				position:            0,
				batchSize:           1,
				consumerGroupMember: -1,
			},
			expErr: ErrInvalidConsumerGroupMember,
		},
		{
			name: "consumer member index out of range",
			config: categoryConfig{
				position:            0,
				batchSize:           1,
				consumerGroupMember: 1,
				consumerGroupSize:   1,
			},
			expErr: ErrInvalidConsumerGroupMember,
		},
		{
			name: "negative consumer group size",
			config: categoryConfig{
				position:          0,
				batchSize:         1,
				consumerGroupSize: -1,
			},
			expErr: ErrInvalidConsumerGroupSize,
		},
		{
			name: "valid",
			config: categoryConfig{
				position:            0,
				batchSize:           1,
				consumerGroupMember: 1,
				consumerGroupSize:   2,
			},
		},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			err := tc.config.validate()
			if !errors.Is(err, tc.expErr) {
				t.Fatalf("expected %v, actual %v", tc.expErr, err)
			}
		})
	}
}

func Test_streamConfig_validate(t *testing.T) {
	testcases := []struct {
		name   string
		config streamConfig
		expErr error
	}{
		{
			name: "invalid stream version",
			config: streamConfig{
				version:   -1,
				batchSize: 1,
			},
			expErr: ErrInvalidReadStreamVersion,
		},
		{
			name: "invalid batch size",
			config: streamConfig{
				version:   0,
				batchSize: 0,
			},
			expErr: ErrInvalidReadBatchSize,
		},
		{
			name: "valid",
			config: streamConfig{
				version:   0,
				batchSize: 1,
			},
		},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			err := tc.config.validate()
			if !errors.Is(err, tc.expErr) {
				t.Fatalf("expected %v, actual %v", tc.expErr, err)
			}
		})
	}
}
