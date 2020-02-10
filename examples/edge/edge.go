package edge

import (
	"strings"
)

type Edge struct {
	Fr, To int
}

type JoinEdge struct {
	Key   int
	Table string
	Edge  Edge
}

type ByKeyThenTable []JoinEdge

func (s ByKeyThenTable) Len() int {
	return len(s)
}

func (s ByKeyThenTable) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s ByKeyThenTable) Less(i, j int) bool {
	if s[i].Key < s[j].Key {
		return true
	}
	if s[i].Key > s[j].Key {
		return false
	}
	if strings.Compare(s[i].Table, s[j].Table) < 0 {
		return true
	}
	return false
}
