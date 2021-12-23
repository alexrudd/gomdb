package locking

import (
	"context"
	"sync"
	"time"

	"github.com/alexrudd/gomdb"
)

type LockState struct {
	stream  gomdb.StreamIdentifier
	version int64
	locks   map[string]time.Time
}

type lockEvent interface {
	lockerID() string
}

func (ls *LockState) apply(lockEvent) {

}

type StreamLocker struct {
	client      *gomdb.Client
	stream      gomdb.StreamIdentifier
	lockerID    string
	idGenerator func() string
	maxLockers  int
	lockTimeout time.Duration

	version     int64
	activeLocks map[string]*activeLock
	mtx         sync.Mutex
}

func NewStreamLocker(client *gomdb.Client) *StreamLocker {
	sl := &StreamLocker{
		client:     client,
		maxLockers: 1,
	}

	return sl
}

// Execute attempts to acquire a lock and execute the provided function. If the
// lock is lost then the function's Context will be cancelled. To stop all
// attempts to acquire a lock cancel the outer Context.
func (sl *StreamLocker) Execute(ctx context.Context, onLocked func(context.Context) error) error {
	// subscribe from last n messages

	errs := make(chan error)
	notifications := make(chan struct{})

	go func() {
		for {
			select {
			case <-ctx.Done():
				errs <- ctx.Err()
				return
			case <-notifications:
				sl.mtx.Lock()
				if len(sl.activeLocks) < sl.maxLockers {
					sl.acquireLock(ctx)
				}
				sl.mtx.Unlock()
			}
		}
	}()

	return nil
}

type lockAcquired struct {
	LockerID string
	Timeout  time.Duration
}

func (sl *StreamLocker) acquireLock(ctx context.Context) error {
	_, err := sl.client.WriteMessage(ctx, sl.stream, gomdb.ProposedMessage{
		ID:   sl.idGenerator(),
		Type: "lockAcquired",
		Data: &lockAcquired{
			LockerID: sl.lockerID,
			Timeout:  sl.lockTimeout,
		},
	}, sl.version)

	return err
}

func (sl *StreamLocker) messageHandler(notify chan<- struct{}, errs chan error) gomdb.MessageHandler {
	event := &lockAcquired{}

	return func(msg *gomdb.Message) {
		err := msg.UnmarshalData(event)
		if err != nil {
			errs <- err
			return
		}

		sl.mtx.Lock()
		defer sl.mtx.Unlock()

		if _, ok := sl.activeLocks[event.LockerID]; ok {
			sl.activeLocks[event.LockerID] = newActiveLock(event.LockerID, notify)
		}

		sl.activeLocks[event.LockerID].extend(msg.Timestamp, event.Timeout)
		notify <- struct{}{}
	}
}

type activeLock struct {
	lockerID string
	timer    *time.Timer
	released chan struct{}
	expired  chan<- struct{}
}

func newActiveLock(lockerID string, expired chan<- struct{}) *activeLock {
	return &activeLock{
		lockerID: lockerID,
		released: make(chan struct{}),
		expired:  expired,
	}
}

func (al *activeLock) extend(from time.Time, timeout time.Duration) {
	al.release()

	// this may cause issues if host clock differs from DB clock.
	al.timer = time.NewTimer(timeout - time.Since(from))

	go func() {
		select {
		case <-al.timer.C:
			al.expired <- struct{}{}
		case <-al.released:
			al.released <- struct{}{}
		}
	}()
}

func (al *activeLock) release() {
	if al.timer == nil {
		return
	}

	// release and wait for confirmation
	al.released <- struct{}{}
	<-al.released

	// stop timer and drain channel
	if !al.timer.Stop() {
		<-al.timer.C
	}

	al.timer = nil
}
