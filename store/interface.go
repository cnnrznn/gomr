package store

type Store interface {
	Read() (any, error)
	Write(v any) error
	Init(c Config) error
	More() bool
}

type Config struct {
	Name string
}
