package store

type Store interface {
	Init(c Config) error
	More() bool
	Read() (any, error)
	Write(v any) error
}

type Config struct {
	Name string // could be filename, could be in-memory store
	Node string // <ip>:<port>
}
