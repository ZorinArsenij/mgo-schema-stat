// Package schema provides schema creation from file with documents generation functionality.
package schema

import (
	"encoding/json"
	"fmt"
	"os"
)

// DB represents database collections.
type DB struct {
	Collections map[string]Collection `json:"collections"`
}

// ParseFromFile parses schema from file.
func ParseFromFile(path string) (*DB, error) {
	var schema DB

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read file: %w", err)
	}

	if err := json.Unmarshal(data, &schema); err != nil {
		return nil, fmt.Errorf("unmarshal schema: %w", err)
	}

	return &schema, nil
}
