package gomr

import (
	"encoding/json"
	"log"
	"net"
	"sync"
	"time"
)

type client struct {
	dst  string
	conn net.Conn
	enc  *json.Encoder
}

type server struct {
	addr     string
	nmappers int
}

func newServer(addy string, nmappers int) *server {
	return &server{
		addr:     addy,
		nmappers: nmappers,
	}
}

func newClient(dst string) *client {
	c := &client{
		dst: dst,
	}
	c.connect()
	c.enc = json.NewEncoder(c.conn)
	return c
}

func handleClient(conn net.Conn, ch chan interface{}, wg *sync.WaitGroup) {
	defer wg.Done()
	dec := json.NewDecoder(conn)
	for dec.More() {
		bs := []byte{}
		if err := dec.Decode(&bs); err != nil {
			log.Panic(err)
		}
		ch <- bs
	}
}

func (s *server) serve() chan interface{} {
	ch := make(chan interface{})

	go func() {
		// Listen for connections
		ln, err := net.Listen("tcp", s.addr)
		if err != nil {
			log.Panic("Error listening:", err)
		}
		defer ln.Close()
		log.Println("Listening at", s.addr)

		var wg sync.WaitGroup
		wg.Add(s.nmappers)

		defer close(ch)
		defer wg.Wait()

		go func() {
			for i := 0; i < s.nmappers; i++ {
				// Open connection
				conn, err := ln.Accept()
				if err != nil {
					log.Panic("Error accept:", err)
				}
				// Write values to ch
				go handleClient(conn, ch, &wg)
			}
		}()
	}()

	return ch
}

func (c *client) transmit(item []byte) {
	if err := c.enc.Encode(item); err != nil {
		c.close()
		log.Panic(err)
	}
}

func (c *client) connect() {
	for i := 0; i < 10; i++ {
		conn, err := net.Dial("tcp", c.dst)
		if err == nil {
			c.conn = conn
			return
		}

		log.Println("Retrying connect:", err)
		time.Sleep(500 * time.Millisecond)
	}

	log.Panic("Could not connect to reducer:", c)
}

func (c *client) close() {
	log.Println("Closing mapper output")
	c.conn.Close()
}
