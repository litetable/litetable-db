package wal

import (
	"bufio"
	"db/internal/litetable"
	"db/internal/protocol"
	"encoding/json"
	"log"
	"os"
)

func (m *Manager) Load(source map[string]map[string]litetable.VersionedQualifier) error {
	filePath, err := m.filePath()
	if err != nil {
		return err
	}

	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			// No WAL file exists yet, not an error
			return nil
		}
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var entry Entry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			log.Println("Skipping malformed WAL entry:", err)
			continue
		}

		// Process based on a protocol operation
		switch entry.Operation {
		case protocol.Read:
			// Read operations don't modify data
			continue

		case protocol.Write:
			// Create a row if it doesn't exist
			if _, exists := source[entry.RowKey]; !exists {
				source[entry.RowKey] = make(map[string]litetable.VersionedQualifier)
			}

			// For each family in the entry
			for family, qualifiers := range entry.Columns {
				// Create a family if it doesn't exist
				if _, exists := source[entry.RowKey][family]; !exists {
					source[entry.RowKey][family] = make(litetable.VersionedQualifier)
				}

				// Append timestamped values for each qualifier
				for qualifier, value := range qualifiers {
					source[entry.RowKey][family][qualifier] = append(
						source[entry.RowKey][family][qualifier],
						litetable.TimestampedValue{
							Value:     value,
							Timestamp: entry.Timestamp,
						},
					)
				}
			}

		case protocol.Delete:
			if entry.Family == "" {
				// Delete the entire row if no family specified
				delete(source, entry.RowKey)
			} else if len(entry.Columns[entry.Family]) == 0 {
				// Delete family if no qualifiers specified
				if row, exists := source[entry.RowKey]; exists {
					delete(row, entry.Family)
				}
			} else {
				// For versioned qualifiers, we add a tombstone entry
				// (nil value with timestamp) to mark as deleted
				if row, exists := source[entry.RowKey]; exists {
					if family, exists := row[entry.Family]; exists {
						for qualifier := range entry.Columns[entry.Family] {
							family[qualifier] = append(family[qualifier],
								litetable.TimestampedValue{
									Value:     nil, // nil indicates deletion
									Timestamp: entry.Timestamp,
								},
							)
						}
					}
				}
			}

		default:
			log.Printf("Unknown operation type: %d, skipping", entry.Operation)
		}
	}

	return scanner.Err()
}
