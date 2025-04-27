package protocol

import (
	"errors"
	"fmt"
)

var (
	ErrInvalidFormat    = errors.New("invalid format")
	ErrUnknownParameter = errors.New("unknown parameter")
	ErrMissingKey       = errors.New("missing search key")
)

// Error wraps a sentinel error with additional context
type Error struct {
	Err     error  // The underlying sentinel error
	Context string // Additional error context
}

// Error satisfies the error interface
func (e *Error) Error() string {
	if e.Context == "" {
		return e.Err.Error()
	}
	return fmt.Sprintf("%s: %s", e.Err.Error(), e.Context)
}

// Unwrap implements the errors.Unwrap interface for compatibility with errors.Is/As
func (e *Error) Unwrap() error {
	return e.Err
}

// newError creates a new protocol error with context
func newError(err error, format string, args ...interface{}) *Error {
	return &Error{
		Err:     err,
		Context: fmt.Sprintf(format, args...),
	}
}
