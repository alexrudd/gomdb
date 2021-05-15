# Go Message DB Client

This module implements a thin Go wrapper around the [Message DB](https://github.com/message-db/message-db) message store. Message DB is an event store implemented on top of Postgres, ideal for event sourcing applications.

The client supports all Message DB read and write procedures, choosing to default to their simplest forms and providing configurability through options functions.

## Getting started

```go
// Open database connection
db, err := sql.Open("postgres", "dbname=message_store sslmode=disable user=message_store")
if err != nil {
    log.Fatalf("unexpected error opening db: %s", err)
}
defer db.Close()

// Set search path for schema
if _, err := db.Exec("SET search_path TO message_store,public;"); err != nil {
    log.Fatalf("setting search path: %s", err)
}

// create client
client := gomdb.NewClient(db)

// read from stream
msgs, err := client.GetStreamMessages(context.Background(), stream)
if err != nil {
    log.Fatalf("reading from stream: %s", err)
}

log.Println(msgs)
```

See the [example](./example) directory for a more complete example.

## Running tests

The unit tests require an instance of Message DB running to test against.

```bash
# start Message DB
docker build -t message-db .
docker run -d --rm \
    -p 5432:5432 \
    -e POSTGRES_HOST_AUTH_METHOD=trust \
    message-db \
    -c message_store.sql_condition=on

# run tests
go test -condition-on
```