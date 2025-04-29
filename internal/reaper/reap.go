package reaper

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// GCParams are the required parameters for the Reapers Garbage Collection process.
type GCParams struct {
	RowKey     string        `json:"rowKey"`
	Family     string        `json:"family"`
	Qualifiers []string      `json:"qualifiers"`
	Timestamp  time.Time     `json:"timestamp"`
	TTL        time.Duration `json:"ttl"`
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
