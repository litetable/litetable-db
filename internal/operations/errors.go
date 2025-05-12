package operations

import (
	"errors"
	"fmt"
)

var (
	errInvalidFormat    = errors.New("invalid format")
	errUnknownParameter = errors.New("unknown parameter")
	errMissingKey       = errors.New("missing search key")
)

// Error wraps a sentinel error with additional context
type Error struct {
	err     error  // The underlying sentinel error
	context string // Additional error context
}

// Error satisfies the error interface
func (e *Error) Error() string {
	if e.context == "" {
		return e.err.Error()
	}
	return fmt.Sprintf("%s: %s", e.err.Error(), e.context)
}

// Unwrap implements the errors.Unwrap interface for compatibility with errors.Is/As
func (e *Error) Unwrap() error {
	return e.err
}

// newError creates a new protocol error with context
func newError(err error, format string, args ...interface{}) *Error {
	return &Error{
		err:     err,
		context: fmt.Sprintf(format, args...),
	}
}
