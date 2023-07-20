package store

type Store interface {
	Read() (string, error)
	Write(v any) error
	Init(c Config) error
	More() bool
}

type Config struct {
	Name string
}
