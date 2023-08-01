package store

import (
	"errors"
	"fmt"
	"net/url"
	"os"
)

type Config struct {
	URL string // use net/url package
}

func Init(c Config) (Store, error) {
	url, err := url.ParseRequestURI(c.URL)
	if err != nil {
		return nil, err
	}

	switch url.Scheme {
	case "file":
		return initFileStore(c)
	default:
		return nil, fmt.Errorf("Storage type not supported")
	}
}

func initFileStore(c Config) (Store, error) {
	url, err := url.ParseRequestURI(c.URL)
	if err != nil {
		return nil, err
	}

	local, err := IsLocal(c)
	if err != nil {
		return nil, err
	}
	if !local {
		return nil, fmt.Errorf("File is not local")
	}

	return &FileStore{
		filename: url.Path,
	}, nil
}

func IsLocal(c Config) (bool, error) {
	url, err := url.ParseRequestURI(c.URL)
	if err != nil {
		return false, err
	}

	_, err = os.Stat(url.Path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("Error stat'ing file")
	}

	return true, nil
}
