// Package protocol defines the protocol of the LiteTable server.
// Used to enforce incoming and outgoing messages.
package protocol

import (
	"errors"
)

const (
	Unknown = iota
	Read
	Write
	Delete
)

var (
	// ErrUnknown is returned when the protocol is unknown
	ErrUnknown = errors.New("unknown litetable protocol")
)

type Message struct {
	msg []byte
}

// Decode decodes a buffer into a litetable protocol message type and returns the payload
func Decode(buf []byte) (int, []byte, error) {
	if len(buf) < 5 { // Minimum length for protocols
		return Unknown, nil, ErrUnknown
	}

	// Check for READ (4 bytes + space)
	if buf[0] == 'R' && buf[1] == 'E' && buf[2] == 'A' && buf[3] == 'D' && buf[4] == ' ' {
		return Read, buf[5:], nil
	}

	// Check for WRITE (5 bytes + space)
	if len(buf) >= 6 && buf[0] == 'W' && buf[1] == 'R' && buf[2] == 'I' && buf[3] == 'T' && buf[4] == 'E' && buf[5] == ' ' {
		return Write, buf[6:], nil
	}

	// Check for DELETE (6 bytes + space)
	if len(buf) >= 7 && buf[0] == 'D' && buf[1] == 'E' && buf[2] == 'L' && buf[3] == 'E' && buf[4] == 'T' && buf[5] == 'E' && buf[6] == ' ' {
		return Delete, buf[7:], nil
	}

	return Unknown, nil, ErrUnknown
}
