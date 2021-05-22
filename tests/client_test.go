package tests

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"strings"
	"testing"

	"github.com/alexrudd/gomdb"
	"github.com/gofrs/uuid"
	_ "github.com/lib/pq"
)

var (
	host          = flag.String("host", "localhost", "the test db host")
	port          = flag.Int("port", 5432, "the test db port")
	dbname        = flag.String("dbname", "message_store", "the message-db database")
	user          = flag.String("user", "message_store", "the user to connect as")
	password      = flag.String("password", "", "the password to use to login")
	sslmode       = flag.String("sslmode", "disable", "the ssl mode to connect with")
	isConditionOn = flag.Bool("condition-on", false, "is the SQL condition feature on")
)

func Init() {
	flag.Parse()
}

// NewClient opens a new DB connection then creates and returns a Client.
func NewClient(t *testing.T) *gomdb.Client {
	t.Helper()

	conn := fmt.Sprintf("host=%s port=%v dbname=%s user=%s sslmode=%s",
		*host, *port, *dbname, *user, *sslmode)

	if *password != "" {
		conn += " password=" + *password
	}

	db, err := sql.Open("postgres", conn)
	if err != nil {
		t.Fatalf("opening db (%s): %s", conn, err)
	}

	t.Cleanup(func() {
		db.Close()
	})

	_, err = db.Exec("SET search_path TO message_store,public;")
	if err != nil {
		t.Fatalf("setting search path: %s", err)
	}

	return gomdb.NewClient(db)
}

// GenUUID returns a unique UUID.
func GenUUID() string {
	return uuid.NewV4().String()
}

// NewTestStream creates a new StreamIdentifier using the provided category
// prefix.
func NewTestStream(category string) gomdb.StreamIdentifier {
	return gomdb.StreamIdentifier{
		Category: category,
		ID:       GenUUID(),
	}
}

// NewTestCategory returns a unique category name
func NewTestCategory(prefix string) string {
	return prefix + strings.ReplaceAll(GenUUID(), "-", "_")
}

// PopulateStream creates the specified number of messages and writes them
// to the specified stream.
func PopulateStream(t *testing.T, client *gomdb.Client, stream gomdb.StreamIdentifier, messages int) {
	t.Helper()

	var (
		version = gomdb.NoStreamVersion
		err     error
	)

	for i := 0; i < messages; i++ {
		version, err = client.WriteMessage(context.TODO(), stream, gomdb.ProposedMessage{
			ID:   GenUUID(),
			Type: "TestMessage",
			Data: "data",
		}, version)
		if err != nil {
			t.Fatal(err)
		}
	}
}

// PopulateCategory creates multiple streams within a single categatory and
// populates them with messages. The actual category is returned.
func PopulateCategory(t *testing.T, client *gomdb.Client, category string, streams, messages int) string {
	t.Helper()

	for i := 0; i < streams; i++ {
		stream := gomdb.StreamIdentifier{
			Category: category,
			ID:       GenUUID(),
		}

		PopulateStream(t, client, stream, messages)
	}

	return category
}
