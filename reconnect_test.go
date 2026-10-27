package mtapi_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/voorz/mtapi"
)

// With AutoReconnect a dropped transport is recovered in place: the
// client re-dials, logs in again, reloads state and restores
// subscriptions, and reads keep working.
func TestAutoReconnectRestoresSession(t *testing.T) {
	fake, addr := startFakeMT5(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	client, err := mtapi.Dial(ctx, mtapi.Config{
		Login: 12345, Password: "pw123", Server: "Fake-MT5", Host: addr,
		AutoReconnect: true,
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}

	reconnected := make(chan struct{}, 1)
	client.OnReconnect(func() {
		select {
		case reconnected <- struct{}{}:
		default:
		}
	})

	if _, err := client.SubscribeQuote(ctx, "EURUSD"); err != nil {
		t.Fatalf("SubscribeQuote: %v", err)
	}
	if _, err := client.Positions(ctx); err != nil {
		t.Fatalf("Positions before drop: %v", err)
	}

	fake.dropConn()

	select {
	case <-reconnected:
	case <-time.After(20 * time.Second):
		t.Fatal("client did not reconnect within 20s")
	}
	if got := fake.logins(); got != 2 {
		t.Fatalf("server saw %d logins, want 2", got)
	}

	// Reads work on the new session.
	if _, err := client.Positions(ctx); err != nil {
		t.Fatalf("Positions after reconnect: %v", err)
	}
	// The subscription was restored: quotes flow again.
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, ok := client.Quote("EURUSD"); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("quote not restored after reconnect")
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Close returns promptly even with reconnect machinery around.
	closed := make(chan struct{})
	go func() {
		client.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("Close hung")
	}
}

// Without AutoReconnect a dropped transport closes the client, as
// before: later calls fail with ErrClosed.
func TestDropWithoutReconnectClosesClient(t *testing.T) {
	fake, addr := startFakeMT5(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := mtapi.Dial(ctx, mtapi.Config{
		Login: 12345, Password: "pw123", Server: "Fake-MT5", Host: addr,
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer client.Close()

	fake.dropConn()

	deadline := time.Now().Add(10 * time.Second)
	for {
		_, err := client.Positions(ctx)
		if errors.Is(err, mtapi.ErrClosed) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("client did not close after drop; last err = %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
