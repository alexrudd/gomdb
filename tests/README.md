# Message DB Client Integration Tests

Clients tests are moved into a separate module to avoid consumers having to import test dependencies.

## Running tests

The client tests require an instance of Message DB running to test against.

```bash
# Start Message DB
docker build -t message-db .
docker run -d --rm \
    -p 5432:5432 \
    -e POSTGRES_HOST_AUTH_METHOD=trust \
    message-db \
    -c message_store.sql_condition=on

# Run tests
go test -condition-on
```