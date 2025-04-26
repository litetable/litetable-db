// Package protocol defines the protocol of the LiteTable server.
// Used to enforce incoming and outgoing messages.
package protocol

import (
	"errors"
)

const (
	// Write requires an action to write to LiteTable
	Write = iota
	Read
	Delete
)

var (
	// ErrUnknown is returned when the protocol is unknown
	ErrUnknown = errors.New("unknown litetable protocol")
)

type Message struct {
	msg []byte
}

// Decode decodes a buffer into a liteserver protocol message
func Decode(buf []byte) (int, error) {
	// Decode the buffer into a protocol message
	// This is a placeholder for actual decoding logic

	// Check for READ (4 bytes)
	if buf[0] == 'R' && buf[1] == 'E' && buf[2] == 'A' && buf[3] == 'D' {
		return Read, nil
	}

	// Check for WRITE (5 bytes)
	if len(buf) >= 5 && buf[0] == 'W' && buf[1] == 'R' && buf[2] == 'I' && buf[3] == 'T' && buf[4] == 'E' {
		return Write, nil
	}

	// Check for DELETE (6 bytes)
	if len(buf) >= 6 && buf[0] == 'D' && buf[1] == 'E' && buf[2] == 'L' && buf[3] == 'E' && buf[4] == 'T' && buf[5] == 'E' {
		return Delete, nil
	}

	return 0, ErrUnknown
}
