package workers

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/voorz/mtapi"
)

type fakeSession struct {
	mu          sync.Mutex
	accountErr  error
	closed      bool
	onReconnect func()
}

func (f *fakeSession) Account(context.Context) (mtapi.Account, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return mtapi.Account{}, f.accountErr
}
func (f *fakeSession) Positions(context.Context) ([]mtapi.Position, error) {
	return nil, nil
}
func (f *fakeSession) Orders(context.Context) ([]mtapi.Order, error) { return nil, nil }
func (f *fakeSession) Deals(context.Context, time.Time, time.Time) ([]mtapi.Deal, error) {
	return nil, nil
}
func (f *fakeSession) OnReconnect(fn func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onReconnect = fn
}
func (f *fakeSession) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}
func (f *fakeSession) setAccountErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.accountErr = err
}
func (f *fakeSession) isClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

type dialStub struct {
	mu    sync.Mutex
	calls int
	fn    func(call int) (session, error)
}

func (d *dialStub) dial(context.Context, mtapi.Config) (session, *mtapi.Client, error) {
	d.mu.Lock()
	d.calls++
	n := d.calls
	d.mu.Unlock()
	s, err := d.fn(n)
	return s, nil, err
}
func (d *dialStub) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.calls
}

func cfg(login uint64) mtapi.Config {
	return mtapi.Config{Login: login, Server: "Test-Server"}
}

func newPoolWith(stub *dialStub) *Pool {
	p := NewPool()
	p.dial = stub.dial
	return p
}

func waitState(t *testing.T, w *Worker, want State) Status {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		st := w.Status()
		if st.State == want {
			return st
		}
		time.Sleep(10 * time.Millisecond)
	}
	st := w.Status()
	t.Fatalf("state = %v, want %v (last error: %q)", st.State, want, st.LastError)
	return st
}

func TestGetOrStartIdempotent(t *testing.T) {
	stub := &dialStub{fn: func(int) (session, error) { return &fakeSession{}, nil }}
	p := newPoolWith(stub)
	defer p.StopAll()

	w1, err := p.GetOrStart(context.Background(), cfg(111))
	if err != nil {
		t.Fatalf("GetOrStart: %v", err)
	}
	w2, err := p.GetOrStart(context.Background(), cfg(111))
	if err != nil {
		t.Fatalf("GetOrStart again: %v", err)
	}
	if w1 != w2 {
		t.Fatal("GetOrStart returned different workers for the same account")
	}
	if stub.count() != 1 {
		t.Fatalf("dial calls = %d, want 1", stub.count())
	}
	if st := w1.Status(); st.State != StateConnected {
		t.Fatalf("state = %v, want connected", st.State)
	}
	if p.Len() != 1 {
		t.Fatalf("pool len = %d, want 1", p.Len())
	}
}

func TestDialAuthFailureParksWorker(t *testing.T) {
	good := &fakeSession{}
	stub := &dialStub{fn: func(call int) (session, error) {
		if call == 1 {
			return nil, fmt.Errorf("%w: bad password", mtapi.ErrAuth)
		}
		return good, nil
	}}
	p := newPoolWith(stub)
	defer p.StopAll()

	w, err := p.GetOrStart(context.Background(), cfg(222))
	if err == nil {
		t.Fatal("GetOrStart with bad credentials: want an error")
	}
	if st := w.Status(); st.State != StateAuthFailed {
		t.Fatalf("state = %v, want auth_failed", st.State)
	}
	// Queries fail fast with the auth error instead of hanging.
	qctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := w.Account(qctx); !errors.Is(err, mtapi.ErrAuth) {
		t.Fatalf("Account error = %v, want ErrAuth", err)
	}
	// Restart after the (simulated) fix redials and connects.
	w.Restart()
	waitState(t, w, StateConnected)
}

func TestQueryOutcomesDriveStatus(t *testing.T) {
	fake := &fakeSession{}
	stub := &dialStub{fn: func(int) (session, error) { return fake, nil }}
	p := newPoolWith(stub)
	defer p.StopAll()

	w, err := p.GetOrStart(context.Background(), cfg(333))
	if err != nil {
		t.Fatalf("GetOrStart: %v", err)
	}

	fake.setAccountErr(fmt.Errorf("%w: boom", mtapi.ErrConnection))
	if _, err := w.Account(context.Background()); err == nil {
		t.Fatal("Account: want the connection error")
	}
	if st := w.Status(); st.State != StateReconnecting || st.LastError == "" {
		t.Fatalf("after transient error: state = %v, lastErr = %q; want reconnecting with an error", st.State, st.LastError)
	}

	fake.setAccountErr(nil)
	if _, err := w.Account(context.Background()); err != nil {
		t.Fatalf("Account after heal: %v", err)
	}
	if st := w.Status(); st.State != StateConnected || st.LastError != "" {
		t.Fatalf("after heal: state = %v, lastErr = %q; want connected, clean", st.State, st.LastError)
	}

	fake.setAccountErr(fmt.Errorf("%w: rejected", mtapi.ErrAuth))
	if _, err := w.Account(context.Background()); err == nil {
		t.Fatal("Account: want the auth error")
	}
	if st := w.Status(); st.State != StateAuthFailed {
		t.Fatalf("after auth error: state = %v, want auth_failed", st.State)
	}
}

func TestClosedSessionIsRedialed(t *testing.T) {
	fakes := []*fakeSession{{}, {}}
	stub := &dialStub{fn: func(call int) (session, error) {
		if call > len(fakes) {
			return &fakeSession{}, nil
		}
		return fakes[call-1], nil
	}}
	p := newPoolWith(stub)
	defer p.StopAll()

	w, err := p.GetOrStart(context.Background(), cfg(444))
	if err != nil {
		t.Fatalf("GetOrStart: %v", err)
	}
	fakes[0].setAccountErr(mtapi.ErrClosed)
	if _, err := w.Account(context.Background()); !errors.Is(err, mtapi.ErrClosed) {
		t.Fatalf("Account error = %v, want ErrClosed", err)
	}
	// The maintenance loop must replace the dead session.
	deadline := time.Now().Add(5 * time.Second)
	for stub.count() < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if stub.count() < 2 {
		t.Fatal("dead session was not redialed")
	}
	waitState(t, w, StateConnected)
	if _, err := w.Account(context.Background()); err != nil {
		t.Fatalf("Account on the new session: %v", err)
	}
}

func TestStopAndStatuses(t *testing.T) {
	stub := &dialStub{fn: func(int) (session, error) { return &fakeSession{}, nil }}
	p := newPoolWith(stub)

	w1, _ := p.GetOrStart(context.Background(), mtapi.Config{Login: 20, Server: "B-Srv"})
	if _, err := p.GetOrStart(context.Background(), mtapi.Config{Login: 10, Server: "A-Srv"}); err != nil {
		t.Fatalf("GetOrStart second account: %v", err)
	}

	sts := p.Statuses()
	if len(sts) != 2 {
		t.Fatalf("statuses = %d, want 2", len(sts))
	}
	if sts[0].Key.Server != "A-Srv" || sts[1].Key.Server != "B-Srv" {
		t.Fatalf("statuses not sorted by server: %v, %v", sts[0].Key, sts[1].Key)
	}

	if !p.Stop(w1.Key()) {
		t.Fatal("Stop: worker not found")
	}
	if p.Len() != 1 {
		t.Fatalf("pool len = %d, want 1", p.Len())
	}
	if st := w1.Status(); st.State != StateClosed {
		t.Fatalf("stopped worker state = %v, want closed", st.State)
	}
	if p.Stop(w1.Key()) {
		t.Fatal("second Stop should report false")
	}
	p.StopAll()
	if p.Len() != 0 {
		t.Fatalf("pool len after StopAll = %d, want 0", p.Len())
	}
}
