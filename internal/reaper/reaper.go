package reaper

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/litetable/litetable-db/internal/litetable"
	"os"
	"time"
)

// GCParams are the required parameters for the Reapers Garbage Collection process.
type GCParams struct {
	RowKey     string    `json:"rowKey"`
	Family     string    `json:"family"`
	Qualifiers []string  `json:"qualifiers"`
	Timestamp  time.Time `json:"timestamp"`
	ExpiresAt  time.Time `json:"expiresAt"`
}

// Reap will take in GCParams and throw it into the Garbage Collector.
func (r *Reaper) Reap(p *GCParams) {
	r.collector <- *p
}

// write will append the GCParams to the GC log file.
func (r *Reaper) write(p *GCParams) {
	// open the file
	file, err := os.OpenFile(r.filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0640)
	if err != nil {
		fmt.Printf("failed to open GC log file: %v", err)
		return
	}

	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			fmt.Printf("failed to close GC log file: %v\n", err)
		}
	}(file)

	data, err := json.Marshal(p)
	if err != nil {
		fmt.Printf("failed to marshal GCParams: %v\n", err)
		return
	}

	_, err = file.WriteString(string(data) + "\n")
	if err != nil {
		fmt.Printf("failed to write GCParams to log file: %v\n", err)
	}
}

// garbageCollector runs the garbage collection over tombstones.
func (r *Reaper) garbageCollector() {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// Open the file
	file, err := os.Open(r.filePath)
	if err != nil {
		fmt.Printf("Error opening GC log file: %v\n", err)
		return
	}
	defer file.Close()

	// Read the file line by line
	var entries []GCParams
	var activeEntries []GCParams
	var processed int
	var removed int

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 {
			continue
		}

		var params GCParams
		if err := json.Unmarshal([]byte(line), &params); err != nil {
			fmt.Printf("Error unmarshaling GC log entry: %v\n", err)
			continue
		}
		entries = append(entries, params)
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading GC log file: %v\n", err)
		return
	}

	// Current time to check expiration
	now := time.Now()

	// Process each entry
	for _, params := range entries {
		processed++

		// Check if it's expired
		if now.After(params.ExpiresAt) {
			// Process the tombstone for this entry
			if deleted := r.didDeleteTombstone(&params); deleted {
				removed++
			} else {
				// the entry is still valid and should remain in the file
				activeEntries = append(activeEntries, params)
			}
		} else {
			// Keep the entry for next time
			activeEntries = append(activeEntries, params)
		}
	}

	// Rewrite the file with only active entries
	if err = r.rewriteGCLog(activeEntries); err != nil {
		fmt.Printf("Error rewriting GC log file: %v\n", err)
	}

	fmt.Printf("Garbage collection complete: processed %d entries, removed %d\n", processed, removed)
}

func (r *Reaper) didDeleteTombstone(params *GCParams) bool {
	data := r.storage.GetData()

	// Check if the row exists
	row, exists := (*data)[params.RowKey]
	if !exists {
		fmt.Printf("Row %s does not exist\n", params.RowKey)
		return true
	}

	// Check if the family exists
	family, exists := row[params.Family]
	if !exists {
		fmt.Printf("Family %s does not exist in row %s\n", params.Family, params.RowKey)
		return true
	}

	// Lock for writing
	r.storage.RWLock()
	defer r.storage.RWUnlock()

	changed := false

	// Process each qualifier
	for _, qualifier := range params.Qualifiers {
		values, exists := family[qualifier]
		if !exists {
			fmt.Printf("Qualifier %s does not exist in family %s\n", qualifier, params.Family)
			continue
		}

		// Filter out entries with timestamp ≤ params.Timestamp
		var newValues []litetable.TimestampedValue
		for _, entry := range values {
			if entry.Timestamp.After(params.Timestamp) {
				newValues = append(newValues, entry)
			} else {
				changed = true
			}
		}

		// Update the qualifier with filtered values or remove it if empty
		if len(newValues) > 0 {
			family[qualifier] = newValues
		} else {
			delete(family, qualifier)
			changed = true
		}
	}

	// Clean up empty structures
	if len(family) == 0 {
		delete(row, params.Family)
	}

	if len(row) == 0 {
		delete(*data, params.RowKey)
	}

	return changed
}

// rewriteGCLog rewrites the GC log file with only active entries.
func (r *Reaper) rewriteGCLog(entries []GCParams) error {
	// Truncate the file (effectively delete all content)
	file, err := os.OpenFile(r.filePath, os.O_WRONLY|os.O_TRUNC, 0640)
	if err != nil {
		return fmt.Errorf("failed to truncate GC log file: %w", err)
	}
	defer file.Close()

	// Write the active entries back to the file
	for _, entry := range entries {
		data, err := json.Marshal(entry)
		if err != nil {
			return fmt.Errorf("failed to marshal entry: %w", err)
		}

		if _, err := file.WriteString(string(data) + "\n"); err != nil {
			return fmt.Errorf("failed to write active entry: %w", err)
		}
	}

	// Ensure data is written to disk
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync GC log file: %w", err)
	}

	return nil
}
