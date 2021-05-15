package gomdb

const (
	// CorrelationKey attribute allows a component to tag an outbound message
	// with its origin
	CorrelationKey = "correlationStreamName"

	// WriteMessageSQL with (
	//   id,
	//   stream_name,
	//   type,
	//   data,
	//   metadata,
	//   expected_version
	// )
	WriteMessageSQL = "SELECT write_message($1, $2, $3, $4, $5, $6)"
	// GetStreamMessagesSQL with (
	//   stream_name,
	//   position,
	//   batch_size,
	//   condition
	// )
	GetStreamMessagesSQL = "SELECT * FROM get_stream_messages($1, $2, $3, $4)"
	// GetCategoryMessagesSQL with (
	//   category_name,
	//   position,
	//   batch_size,
	//   correlation,
	//   consumer_group_member,
	//   consumer_group_size,
	//   condition
	// )
	GetCategoryMessagesSQL = "SELECT * FROM get_category_messages($1, $2, $3, $4, $5, $6, $7)"
	// GetLastStreamMessageSQL with (stream_name)
	GetLastStreamMessageSQL = "SELECT * FROM get_last_stream_message($1)"
	// StreamVersionSQL with (stream_name)
	GetStreamVersionSQL = "SELECT * FROM stream_version($1)"
)
