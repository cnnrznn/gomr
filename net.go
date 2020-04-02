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

func handlePipe(conn net.Conn, ch chan []byte, wg *sync.WaitGroup) {
	defer wg.Done()
	buf := make([]byte, 4096)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			return
		}
		ch <- buf[:n]
	}
}

func (s *Server) Serve() chan []byte {
	ch := make(chan []byte)

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
				}
				// Write values to ch
				go handlePipe(conn, ch, &wg)
			}
		}()
	}()

	return ch
}

func (p *Pipe) Transmit(item interface{}) {
	if !p.connected {
		p.Connect()
	}

	bs, err := json.Marshal(item)
	if err != nil {
		log.Println("Error marshalling:", err)
	}

	p.conn.Write(bs)
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
