package store

import (
	"fmt"
)

type MemStore struct {
	Data []any
	curr int
}

func (m *MemStore) Init(config Config) error {
	m.curr = 0
	return nil
}

func (m *MemStore) More() bool {
	if m.curr < len(m.Data) {
		return true
	}
	return false
}

func (m *MemStore) Read() (any, error) {
	if m.curr >= len(m.Data) {
		return "", fmt.Errorf("Reading past buffer")
	}

	entry := m.Data[m.curr]
	m.curr++

	return entry, nil
}

func (m *MemStore) Write(v any) error {
	m.Data = append(m.Data, v)

	return nil
}
