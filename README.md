# Go Message DB Client

[![Checks](https://github.com/alexrudd/gomdb/actions/workflows/checks.yml/badge.svg?event=push)](https://github.com/alexrudd/gomdb/actions/workflows/checks.yml)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/alexrudd/gomdb)](https://pkg.go.dev/github.com/alexrudd/gomdb)

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

// Create client
client := gomdb.NewClient(db)

// Read from stream
msgs, err := client.GetStreamMessages(context.Background(), stream)
if err != nil {
    log.Fatalf("reading from stream: %s", err)
}

log.Println(msgs)
```

See the [examples](./tests/example) or [tests](./tests) directory for more complete examples.

## Subscriptions

Subscriptions are built on top of the `GetStreamMessages` and `GetCategoryMessages` methods and simply poll from the last read version or position.

```go
subCtx, cancel := context.WithCancel(context.Background())
defer cancel() // cancel will stop the subscription

err := client.SubscribeToCategory(subCtx, "user",
    func(m *gomdb.Message) { // Message handler
        log.Printf("Received message: %v", m)
    },
    func(live bool) { // Liveness handler
        if live {
            log.Print("subscription is handling live messages!")
        } else {
            log.Print("subscription has fallen behind")
        }
    },
    func(err error) { // subscription dropped handler
        if err != nil {
            log.Fatalf("subscription dropped with error: %s", err)
        }
    },
)
if err != nil {
    log.Fatal(err)
}
```

The client can be configured with different polling strategies to reduce reads to the database for subscriptions that rarely receive messages

```go
// Client configured with exponential backoff
client := gomdb.NewClient(
    db,
    gomdb.WithSubPollingStrategy(
        gomdb.ExpBackoffPolling(
            50*time.Millisecond, // minimum polling delay on no messages read
            5*time.Second,       // maximum polling delay on no messages read
            2,                   // delay will double for every read that returns no messages
        ),
    ),
)

// Client configured with constant polling interval
client = gomdb.NewClient(
    db,
    gomdb.WithSubPollingStrategy(
        gomdb.ConstantPolling(100*time.Millisecond), // polling delay on no messages read
    ),
)
```

## Running tests

The unit tests can be run with `go test`.

See the [integration tests README](./tests/README.md) for instructions on how to run integration tests.

## Contributing

All contributions welcome, especially anyone with SQL experience who could tidy up how queries are run and how read errors are handled.
