package store

import (
	"fmt"
	"net/url"
)

type Store interface {
	More() bool
	Read() ([]byte, error)
	Write(bs []byte) error
}

type Config struct {
	URL string // use net/url package
}

func Init(c Config) (Store, error) {
	// initialize, migrate, etc. new datastore
	// based on the url in the config
	url, err := url.ParseRequestURI(c.URL)
	if err != nil {
		return nil, err
	}

	switch url.Scheme {
	case "file":
		return initFileStore(url)
	default:
		return nil, fmt.Errorf("Storage type not supported")
	}
}

func initFileStore(url *url.URL) (Store, error) {
	// if on this machine, open
	// if on another machine, copy here
	return nil, fmt.Errorf("Not implemented")
}
