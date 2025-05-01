package litetable

import (
	"time"
)

type Operation string

const (
	// OperationRead represents a read operation
	OperationRead Operation = "READ"
	// OperationWrite represents a write operation
	OperationWrite Operation = "WRITE"
	// OperationDelete represents a delete operation
	OperationDelete Operation = "DELETE"
	// OperationCreate represents a create operation
	OperationCreate Operation = "CREATE"
	// OperationUnknown represents an unknown operation
	OperationUnknown Operation = "UNKNOWN"
)

// TimestampedValue stores a value with its timestamp
type TimestampedValue struct {
	Value       []byte    `json:"value"`
	Timestamp   time.Time `json:"timestamp"`
	IsTombstone bool      `json:"tombstone,omitempty"` // if the value is slated for deletion
	ExpiresAt   time.Time `json:"expiresAt,omitempty"` // the time in which the value will expire
}

// VersionedQualifier maps qualifiers to their timestamped values
type VersionedQualifier map[string][]TimestampedValue // family → qualifier → []TimestampedValue

// Row defines a row of data in LiteTable:
//
// Example:
//
//	Row{
//	  Key: "row1",
//	  Columns: map[string]Qualifier{
//	    "family1": {
//	      "qualifier1": []byte("value1"),
//	      "qualifier2": []byte("value2"),
//	    },
//	    "family2": {
//	      "qualifier1": []byte("value3"),
//	    },
//	  },
//	}
//	This represents a row with key "row1" containing two families: "family1" and "family2",
//	each with their respective qualifiers and values.
//
// Qualifiers are defined by your codes' logic.
type Row struct {
	Key string `json:"key"`
	// family → qualifier → []TimestampedValue
	Columns map[string]VersionedQualifier `json:"cols,omitempty"`
}

type Data map[string]map[string]VersionedQualifier
