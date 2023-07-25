package store

import (
	"fmt"
)

type MemStore struct {
	data [][]byte
	curr int
}

func (m *MemStore) More() bool {
	if m.curr < len(m.data) {
		return true
	}
	return false
}

func (m *MemStore) Read() ([]byte, error) {
	if m.curr >= len(m.data) {
		return nil, fmt.Errorf("Reading past buffer")
	}

	entry := m.data[m.curr]
	m.curr++

	return entry, nil
}

func (m *MemStore) Write(bs []byte) error {
	m.data = append(m.data, bs)
	return nil
}
