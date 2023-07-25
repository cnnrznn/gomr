package store

import (
	"fmt"
)

type MemStore struct {
	Data [][]byte
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

func (m *MemStore) Read() ([]byte, error) {
	if m.curr >= len(m.Data) {
		return nil, fmt.Errorf("Reading past buffer")
	}

	entry := m.Data[m.curr]
	m.curr++

	return entry, nil
}

func (m *MemStore) Write(bs []byte) error {
	m.Data = append(m.Data, bs)
	return nil
}
