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
			// Process write operations normally
			if _, exists := source[entry.RowKey]; !exists {
				source[entry.RowKey] = make(map[string]litetable.VersionedQualifier)
			}

			for family, qualifiers := range entry.Columns {
				if _, exists := source[entry.RowKey][family]; !exists {
					source[entry.RowKey][family] = make(litetable.VersionedQualifier)
				}

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
			// Instead of adding tombstones, directly prune the data from memory
			if entry.Family == "" {
				// Delete entire row
				delete(source, entry.RowKey)
			} else if row, exists := source[entry.RowKey]; exists {
				if len(entry.Columns[entry.Family]) == 0 {
					// Delete entire family
					delete(row, entry.Family)
					// Clean up empty row
					if len(row) == 0 {
						delete(source, entry.RowKey)
					}
				} else {
					// Delete specific qualifiers
					if family, exists := row[entry.Family]; exists {
						for qualifier := range entry.Columns[entry.Family] {
							delete(family, qualifier)
						}
						// Clean up empty family
						if len(family) == 0 {
							delete(row, entry.Family)
							// Clean up empty row
							if len(row) == 0 {
								delete(source, entry.RowKey)
							}
						}
					}
				}
			}
		}
	}

	return scanner.Err()
}
