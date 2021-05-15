package gomdb

import "errors"

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

type streamConfig struct {
	version   int64
	batchSize int64
	condition string
}

func (cfg *streamConfig) validate() error {
	if cfg.version < 0 {
		return errors.New("stream version cannot be less than 0")
	} else if cfg.batchSize < 1 {
		return errors.New("batch size must be greater than 0")
	}

	return nil
}

func (cfg *streamConfig) getCondition() interface{} {
	if cfg.condition == "" {
		return nil
	}

	return cfg.condition
}

func newDefaultStreamConfig() *streamConfig {
	return &streamConfig{
		version:   0,
		batchSize: 1000,
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

type categoryConfig struct {
	position            int64
	batchSize           int64
	correlation         string
	consumerGroupMember int64
	consumerGroupSize   int64
	condition           string
}

func newDefaultCategoryConfig() *categoryConfig {
	return &categoryConfig{
		position:  0,
		batchSize: 1000,
	}
}

func (cfg *categoryConfig) validate() error {
	if cfg.position < 0 {
		return errors.New("stream version cannot be less than 0")
	} else if cfg.batchSize < 1 {
		return errors.New("batch size must be greater than 0")
	} else if cfg.consumerGroupMember < 0 {
		return errors.New("consumer group member must be 0 or greater")
	} else if cfg.consumerGroupSize < 0 {
		return errors.New("consumer group size must be 0 or greater (0 to disbale consumer groups)")
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
