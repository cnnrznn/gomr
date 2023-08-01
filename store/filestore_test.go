package store

import (
	"fmt"
	"os"
	"testing"
)

func TestFileStore(t *testing.T) {
	fs := &FileStore{
		filename: "test.txt",
	}

	f, err := os.OpenFile(fs.filename, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		t.Error(err)
	}
	f.Write([]byte("wow!\n"))
	f.Close()

	err = fs.Init()
	if err != nil {
		t.Error(err.Error())
	}

	fs.Write([]byte("This is a line"))
	fs.Write([]byte("another line"))
	fs.Write([]byte("This is a line"))
	fs.Init()

	for fs.More() {
		bs, _ := fs.Read()
		fmt.Println(string(bs))
	}
}
