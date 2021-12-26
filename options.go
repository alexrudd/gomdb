package gomdb

import (
	"errors"
	"math"
	"time"
)

var (
	// ErrInvalidReadStreamVersion is returned when the stream version inside a
	// read call is less than zero.
	ErrInvalidReadStreamVersion = errors.New("stream version cannot be less than 0")
	// ErrInvalidReadBatchSize is returned when the batch size inside a read
	// call is less than one.
	ErrInvalidReadBatchSize = errors.New("batch size must be greater than 0")
	// ErrInvalidReadPosition is returned when the stream position inside a
	// read call is less than zero.
	ErrInvalidReadPosition = errors.New("stream position cannot be less than 0")
	// ErrInvalidConsumerGroupMember is returned when the consumer group ID
	// index is either less than zero or greater than or equal to the consumer
	// group size.
	ErrInvalidConsumerGroupMember = errors.New("consumer group member must be >= 0 < group size")
	// ErrInvalidConsumerGroupSize is returned when the consumer group size is
	// less that zero.
	ErrInvalidConsumerGroupSize = errors.New("consumer group size must be 0 or greater (0 to disbale consumer groups)")
)

// ClientOption is an option for modifiying how the Message DB client operates.
type ClientOption func(*Client)

// WithDefaultPollingStrategy configures to use the specified PollingStrategy
// as the default for new subscriptions.
func WithDefaultPollingStrategy(strat func() PollingStrategy) ClientOption {
	return func(c *Client) {
		c.defaultPollingStrat = strat
	}
}

// PollingStrategy returns the delay duration before the next polling attempt
// based on how many messages were returned from the previous poll vs how many
// were expected.
type PollingStrategy func(retrieved, expected int64) time.Duration

// ExpBackoffPolling returns an exponential polling backoff strategy that starts
// at the min duration but is multipled for every read that did not return
// any messages up to the max duration. The backoff duration is reset to min
// everytime a message is read.
func ExpBackoffPolling(min, max time.Duration, multiplier float64) func() PollingStrategy {
	return func() PollingStrategy {
		noMessageCount := 0
		return func(retrieved, expected int64) time.Duration {
			if retrieved == expected {
				noMessageCount = 0
				return time.Duration(0)
			} else if retrieved > 0 {
				noMessageCount = 0
				return min
			}

			backoff := time.Duration(math.Pow(multiplier, float64(noMessageCount))) * min
			noMessageCount++

			if backoff > max {
				return max
			}

			return backoff
		}
	}
}

// DynamicPolling returns a factory for a PollingStrategy that will dynamically
// adjust a subscription's polling delay by the step amount so as to hit a
// target read utilisation.
// Read utilisation is the number of messages retreived over the number of
// messages expoected.
func DynamicPolling(target float64, step, min, max time.Duration) func() PollingStrategy {
	if target > 1 || target <= 0 {
		panic("target percentage must be in the range: 0>n<=1")
	}

	return func() PollingStrategy {
		var (
			delay  = min
			actual = float64(0)
		)

		return func(retrieved, expected int64) time.Duration {
			if retrieved == expected {
				return time.Duration(0)
			}

			actual = float64(retrieved) / float64(expected)

			// adjust appropriately to reach target
			if actual < target {
				delay += step
			} else if actual > target {
				delay -= step
			}

			// clamp between max/min
			if delay > max {
				delay = max
			} else if delay < min {
				delay = min
			}

			return delay
		}
	}
}

// ConstantPolling returns a constant interval polling strategy
func ConstantPolling(interval time.Duration) func() PollingStrategy {
	return func() PollingStrategy {
		return func(retrieved, expected int64) time.Duration {
			if retrieved == expected {
				return time.Duration(0)
			}

			return interval
		}
	}
}

// GetStreamOption is an option for modifiying how to read from a stream.
type GetStreamOption func(*streamConfig)

// FromVersion specifies the inclusive version from which to read messages.
func FromVersion(version int64) GetStreamOption {
	return func(cfg *streamConfig) {
		cfg.version = version
	}
}

// WithStreamBatchSize specifies the batch size to read messages.
func WithStreamBatchSize(batchSize int64) GetStreamOption {
	return func(cfg *streamConfig) {
		cfg.batchSize = batchSize
	}
}

// WithStreamCondition specifies an SQL condition to apply to the read request.
// For example: "messages.time::time >= current_time"
func WithStreamCondition(condition string) GetStreamOption {
	return func(cfg *streamConfig) {
		cfg.condition = condition
	}
}

// WithStreamPollingStrategy sets the polling strategy for this stream
// subscription. Polling Strategies are only used in subscriptions.
func WithStreamPollingStrategy(strat PollingStrategy) GetStreamOption {
	return func(cfg *streamConfig) {
		cfg.pollingStrat = strat
	}
}

type streamConfig struct {
	version      int64
	batchSize    int64
	condition    string
	pollingStrat PollingStrategy
}

func (cfg *streamConfig) validate() error {
	if cfg.version < 0 {
		return ErrInvalidReadStreamVersion
	} else if cfg.batchSize < 1 {
		return ErrInvalidReadBatchSize
	}

	return nil
}

func (cfg *streamConfig) getCondition() interface{} {
	if cfg.condition == "" {
		return nil
	}

	return cfg.condition
}

func newDefaultStreamConfig(strat PollingStrategy) *streamConfig {
	return &streamConfig{
		version:      0,
		batchSize:    1000,
		pollingStrat: strat,
	}
}

// GetCategoryOption is an option for modifiying how to read from a category.
type GetCategoryOption func(*categoryConfig)

// FromPosition specifies the inclusive global position from which to read
// messages.
func FromPosition(position int64) GetCategoryOption {
	return func(cfg *categoryConfig) {
		cfg.position = position
	}
}

// WithCategoryBatchSize specifies the batch size to read messages.
func WithCategoryBatchSize(batchSize int64) GetCategoryOption {
	return func(cfg *categoryConfig) {
		cfg.batchSize = batchSize
	}
}

// AsConsumerGroup specifies the consumer group options for this read. Size is
// used to specify the number of consumers, and member specifies which consumer
// is currently reading. Message-db used consistent hashing on stream names
// within a category and then distributes the streams amoungst the consumer
// group members.
func AsConsumerGroup(member, size int64) GetCategoryOption {
	return func(cfg *categoryConfig) {
		cfg.consumerGroupMember = member
		cfg.consumerGroupSize = size
	}
}

// WithCorrelation sets the correlation value that messages will be filtered by.
// correlation is compared against each messages medatadata
// correlationStreamName field.
func WithCorrelation(correlation string) GetCategoryOption {
	return func(cfg *categoryConfig) {
		cfg.correlation = correlation
	}
}

// WithCategoryCondition specifies an SQL condition to apply to the read
// request. For example: "messages.time::time >= current_time"
func WithCategoryCondition(condition string) GetCategoryOption {
	return func(cfg *categoryConfig) {
		cfg.condition = condition
	}
}

// WithCategoryPollingStrategy sets the polling strategy for this category
// subscription. Polling Strategies are only used in subscriptions.
func WithCategoryPollingStrategy(strat PollingStrategy) GetCategoryOption {
	return func(cfg *categoryConfig) {
		cfg.pollingStrat = strat
	}
}

type categoryConfig struct {
	position            int64
	batchSize           int64
	correlation         string
	consumerGroupMember int64
	consumerGroupSize   int64
	condition           string
	pollingStrat        PollingStrategy
}

func newDefaultCategoryConfig(strat PollingStrategy) *categoryConfig {
	return &categoryConfig{
		position:     0,
		batchSize:    1000,
		pollingStrat: strat,
	}
}

func (cfg *categoryConfig) validate() error {
	if cfg.position < 0 {
		return ErrInvalidReadPosition
	} else if cfg.batchSize < 1 {
		return ErrInvalidReadBatchSize
	} else if cfg.consumerGroupMember < 0 || (cfg.consumerGroupSize > 0 && cfg.consumerGroupMember >= cfg.consumerGroupSize) {
		return ErrInvalidConsumerGroupMember
	} else if cfg.consumerGroupSize < 0 {
		return ErrInvalidConsumerGroupSize
	}

	return nil
}

func (cfg *categoryConfig) getConsumerGroupMember() interface{} {
	if cfg.consumerGroupSize == 0 {
		return nil
	}

	return cfg.consumerGroupMember
}

func (cfg *categoryConfig) getConsumerGroupSize() interface{} {
	if cfg.consumerGroupSize == 0 {
		return nil
	}

	return cfg.consumerGroupSize
}

func (cfg *categoryConfig) getCorrelation() interface{} {
	if cfg.correlation == "" {
		return nil
	}

	return cfg.correlation
}

func (cfg *categoryConfig) getCondition() interface{} {
	if cfg.condition == "" {
		return nil
	}

	return cfg.condition
}
