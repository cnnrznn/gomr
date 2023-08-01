package store

type Store interface {
	More() bool
	Read([]byte) error
	Write(bs []byte) error
}
