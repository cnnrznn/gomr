package store

type Store interface {
	Init(c Config) error
	More() bool
	Read() ([]byte, error)
	Write(bs []byte) error
}

type Config struct {
	Name string // could be filename, could be in-memory store
	Node string // <ip>:<port>
}
