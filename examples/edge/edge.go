package edge

import (
	"strings"
)

type Edge struct {
	Fr int `json:"fr"`
	To int `json:"to"`
}

type JoinEdge struct {
	JoinKey int    `json:"key"`
	Table   string `json:"table"`
	Edge    Edge   `json:"edge"`
}

func (je JoinEdge) Key() interface{} {
	return je.JoinKey
}

type ByKeyThenTable []JoinEdge

func (s ByKeyThenTable) Len() int {
	return len(s)
}

func (s ByKeyThenTable) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s ByKeyThenTable) Less(i, j int) bool {
	if s[i].JoinKey < s[j].JoinKey {
		return true
	}
	if s[i].JoinKey > s[j].JoinKey {
		return false
	}
	if strings.Compare(s[i].Table, s[j].Table) < 0 {
		return true
	}
	return false
}
