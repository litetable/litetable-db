package wal

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/litetable/litetable-db/internal/protocol"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultWALFile = "wal.log"
)

// Entry represents a Write-Ahead Log entry for a database operation
type Entry struct {
	Operation   int                          `json:"operation"`
	RowKey      string                       `json:"rowKey"`
	Family      string                       `json:"family"`
	Columns     map[string]map[string][]byte `json:"cols"` // family -> qualifier -> value
	Timestamp   time.Time                    `json:"timestamp"`
	IsTombstone bool                         `json:"isTombstone,omitempty"`
	ExpiresAt   *time.Time                   `json:"expiresAt,omitempty"`
}

type Manager struct {
	mu      sync.RWMutex
	walFile *os.File
	path    string
}

type Config struct {
	// Path to the WAL directory
	Path string
}

func (c *Config) validate() error {
	var errGrp []error
	// Path is optional, so no validation needed
	return errors.Join(errGrp...)
}

func New(cfg *Config) (*Manager, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	var walPath string

	if cfg.Path == "" {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return nil, errors.New("failed to get home directory: " + err.Error())
		}
		walPath = filepath.Join(homeDir, ".litetable/wal", defaultWALFile)
	} else {
		walPath = cfg.Path
	}

	// Ensure WAL directory exists
	walDir := filepath.Dir(walPath)
	if err := os.MkdirAll(walDir, 0750); err != nil {
		return nil, errors.New("failed to create WAL directory: " + err.Error())
	}

	// Open WAL file with appropriate permissions
	file, err := os.OpenFile(walPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0640)
	if err != nil {
		return nil, errors.New("failed to open WAL file: " + err.Error())
	}

	return &Manager{
		walFile: file,
		path:    walPath,
	}, nil
}

// Apply takes in the query bytes and appends to the WAL file:
//
// ex: key=testKey:12345 family=main qualifier=status value=active qualifier=time value=now
//
// WAL is written locally to allow replaying the log in case of failure. If the WAL is lost
// or corrupted, the data is still available in the database. The WAL is used to ensure
// that the data is written to the database before the transaction is considered complete.
func (m *Manager) Apply(msgType int, b []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if msgType == protocol.Read {
		return nil
	}

	entry, err := m.parse(msgType, b)
	if err != nil {
		return fmt.Errorf("failed to parse WAL entry: %w", err)
	}

	// Convert the entry to JSON for storage
	jsonData, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal entry: %w", err)
	}

	// Write the JSON data to the WAL file, followed by a newline
	if _, err = m.walFile.Write(append(jsonData, '\n')); err != nil {
		return fmt.Errorf("failed to write to WAL: %w", err)
	}

	return nil
}

func (m *Manager) parse(msgType int, b []byte) (*Entry, error) {
	input := string(b)
	parts := splitParts(input)

	entry := &Entry{
		Operation:   msgType,
		Columns:     make(map[string]map[string][]byte),
		Timestamp:   time.Now(),
		IsTombstone: msgType == protocol.Delete, // Set tombstone flag for delete operations
	}

	// Special handling for CREATE operation
	if msgType == protocol.Create {
		// Parse the family=columns part
		for i := 0; i < len(parts); i++ {
			kv := strings.SplitN(parts[i], "=", 2)
			if len(kv) != 2 {
				return nil, fmt.Errorf("invalid format at %s", parts[i])
			}

			key, value := kv[0], kv[1]

			if key == "family" {
				// Store the raw family value in the WAL entry
				entry.Family = value

				// Split the family names and create entries for each
				familyNames := strings.Split(value, ",")
				for _, familyName := range familyNames {
					familyName = strings.TrimSpace(familyName)
					if familyName == "" {
						continue
					}

					// Each family name gets its own column map in the WAL
					entry.Columns[familyName] = make(map[string][]byte)
				}
			}
		}

		if entry.Family == "" {
			return nil, fmt.Errorf("missing family name")
		}

		if len(entry.Columns) == 0 {
			return nil, fmt.Errorf("missing columns in family definition")
		}

		return entry, nil
	}

	// Existing code for other operations
	var (
		family           string
		currentQualifier string
		ttl              time.Duration
	)

	for i := 0; i < len(parts); i++ {
		kv := strings.SplitN(parts[i], "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid format at %s", parts[i])
		}

		key, value := kv[0], kv[1]
		key = strings.TrimLeft(key, "-")

		// Decode URL-encoded values
		decodedValue, err := url.QueryUnescape(value)
		if err != nil {
			return nil, fmt.Errorf("failed to decode value: %s", err)
		}

		switch key {
		case "key":
			entry.RowKey = decodedValue
		case "family":
			family = decodedValue
			entry.Family = decodedValue
			entry.Columns[family] = make(map[string][]byte)
		case "qualifier":
			currentQualifier = decodedValue
			// For delete operations, we explicitly mark the qualifier in the WAL
			if msgType == protocol.Delete && family != "" {
				if _, ok := entry.Columns[family]; !ok {
					entry.Columns[family] = make(map[string][]byte)
				}
				// Use an empty byte array to represent the tombstone
				entry.Columns[family][currentQualifier] = nil
			}
		case "value":
			if family == "" {
				return nil, fmt.Errorf("value without family")
			}
			if currentQualifier == "" {
				return nil, fmt.Errorf("value without qualifier")
			}

			if _, ok := entry.Columns[family]; !ok {
				entry.Columns[family] = make(map[string][]byte)
			}

			entry.Columns[family][currentQualifier] = []byte(decodedValue)
			currentQualifier = "" // Reset for next qualifier
		case "ttl":
			// For delete operations, capture TTL
			if msgType == protocol.Delete {
				ttlSec, err := strconv.ParseInt(decodedValue, 10, 64)
				if err != nil {
					return nil, fmt.Errorf("invalid ttl value: %s", decodedValue)
				}
				ttl = time.Duration(ttlSec) * time.Second
			}
		default:
			// Ignore other parameters for WAL purposes
		}
	}

	// Modify validation for other operations
	if entry.RowKey == "" {
		return nil, fmt.Errorf("missing key")
	}

	// Set expiration time if TTL is provided
	if msgType == protocol.Delete && ttl > 0 {
		expiresAt := entry.Timestamp.Add(ttl)
		entry.ExpiresAt = &expiresAt
	}

	// For delete operations, family might be empty if deleting an entire row
	if msgType != protocol.Delete && entry.Family == "" {
		return nil, fmt.Errorf("missing family")
	}

	// For delete operations, we may not have qualifiers
	if msgType != protocol.Delete &&
		(len(entry.Columns) == 0 || (family != "" && len(entry.Columns[family]) == 0)) {
		return nil, fmt.Errorf("missing qualifier/value pairs")
	}

	return entry, nil
}

// splitParts splits the input string by spaces, but preserves spaces within quoted values
func splitParts(input string) []string {
	var parts []string
	var currentPart strings.Builder
	inQuotes := false

	for i := 0; i < len(input); i++ {
		c := input[i]

		if c == '"' {
			inQuotes = !inQuotes
			currentPart.WriteByte(c)
		} else if c == ' ' && !inQuotes {
			// End of part
			if currentPart.Len() > 0 {
				parts = append(parts, currentPart.String())
				currentPart.Reset()
			}
		} else {
			currentPart.WriteByte(c)
		}
	}

	// Add the last part if it exists
	if currentPart.Len() > 0 {
		parts = append(parts, currentPart.String())
	}

	return parts
}

// filePath returns the location of the WAL file
func (m *Manager) filePath() (string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", errors.New("failed to get home directory: " + err.Error())
	}
	walPath := filepath.Join(homeDir, ".litetable/wal", defaultWALFile)

	return walPath, nil
}
