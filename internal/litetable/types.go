package litetable

import (
	"time"
)

// TimestampedValue stores a value with its timestamp
type TimestampedValue struct {
	Value     []byte    `json:"value"`
	Timestamp time.Time `json:"timestamp"`
}

// VersionedQualifier maps qualifiers to their timestamped values
type VersionedQualifier map[string][]TimestampedValue

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
	Key     string                        `json:"key"`
	Columns map[string]VersionedQualifier `json:"cols"` // family → qualifier → []TimestampedValue
}

// GetLatest returns the most recent value for a qualifier
func GetLatest(values []TimestampedValue) ([]byte, time.Time, bool) {
	if len(values) == 0 {
		return nil, time.Time{}, false
	}

	latest := values[0]
	for _, v := range values {
		if v.Timestamp.After(latest.Timestamp) {
			latest = v
		}
	}

	return latest.Value, latest.Timestamp, true
}
