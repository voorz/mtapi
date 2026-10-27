package mtapi

import (
	"errors"
	"fmt"
)

// Sentinel errors of the package. Returned errors wrap one of these
// (or are a *TradeError) so callers can classify failures with
// errors.Is / errors.As. The set mirrors the reference implementation's
// exception hierarchy (MT5Error, AuthError, TradeError,
// ServerNotFoundError, ConnectionError, TimeoutError).
var (
	// ErrServerNotFound reports that a server name could not be
	// resolved to any access point.
	ErrServerNotFound = errors.New("mtapi: server not found")
	// ErrAuth reports a failed auth handshake or login.
	ErrAuth = errors.New("mtapi: authentication failed")
	// ErrConnection reports a transport-level failure.
	ErrConnection = errors.New("mtapi: connection failed")
	// ErrTimeout reports that an operation exceeded its deadline.
	ErrTimeout = errors.New("mtapi: operation timed out")
	// ErrClosed reports use of a client after Close, or a connection
	// that dropped mid-operation.
	ErrClosed = errors.New("mtapi: client is closed")
	// ErrSymbolNotFound reports an unknown symbol name.
	ErrSymbolNotFound = errors.New("mtapi: symbol not found")
	// ErrNoQuote reports a trade attempted before any quote arrived
	// for the symbol (subscribe first).
	ErrNoQuote = errors.New("mtapi: no quote for symbol (subscribe first)")
)

// TradeError is a failed trade operation carrying the MT5 retcode
// returned by the server (for example 10030, "position not found",
// raised locally when a ticket cannot be resolved).
type TradeError struct {
	Retcode int
	Message string
}

func (e *TradeError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("mtapi: trade error %d: %s", e.Retcode, e.Message)
	}
	return fmt.Sprintf("mtapi: trade error %d", e.Retcode)
}

// authError builds an error wrapping ErrAuth.
func authError(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrAuth, fmt.Sprintf(format, args...))
}

// connError builds an error wrapping ErrConnection.
func connError(err error) error {
	return fmt.Errorf("%w: %v", ErrConnection, err)
}
