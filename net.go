package gomr

import (
	"encoding/json"
	"log"
	"net"
	"sync"
)

type Pipe struct {
	dst       string
	connected bool
	conn      net.Conn
}

type Server struct {
	addr   string
	npeers int
}

func NewServer(addy string, npeers int) *Server {
	return &Server{
		addr:   addy,
		npeers: npeers,
	}
}

func NewPipe(dst string) *Pipe {
	pipe := &Pipe{
		dst:       dst,
		connected: false,
	}
	pipe.Connect()
	return pipe
}

func handlePipe(conn net.Conn, ch chan interface{}, wg *sync.WaitGroup) {
	defer wg.Done()
	dec := json.NewDecoder(conn)
	for dec.More() {
		m := make(map[string]interface{})
		dec.Decode(&m)
		bs, err := json.Marshal(m)
		if err != nil {
			log.Println("Error marshal:", err)
		}
		ch <- bs
	}
}

func (s *Server) Serve() chan interface{} {
	ch := make(chan interface{})

	go func() {
		// Listen for connections
		ln, err := net.Listen("tcp", s.addr)
		if err != nil {
			log.Println("Error listening:", err)
		}
		defer ln.Close()
		log.Println("Listening at", s.addr)

		var wg sync.WaitGroup
		wg.Add(s.npeers)
		defer close(ch)
		defer wg.Wait()

		go func() {
			for {
				// Open connection
				conn, err := ln.Accept()
				if err != nil {
					log.Println("Error accept:", err)
					break
				}
				// Write values to ch
				go handlePipe(conn, ch, &wg)
			}
		}()
	}()

	return ch
}

func (p *Pipe) Transmit(item []byte) {
	if !p.connected {
		p.Connect()
	}

	p.conn.Write(item)
}

func (p *Pipe) Connect() {
	for {
		conn, err := net.Dial("tcp", p.dst)
		if err == nil {
			p.conn = conn
			p.connected = true
			return
		} else {
			log.Println("Could not connect:", err)
		}
	}
}

func (p *Pipe) Close() {
	p.conn.Close()
	p.connected = false
}
