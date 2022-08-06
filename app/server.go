package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Command string

const (
	CommandPing Command = "PING"
	CommandEcho Command = "ECHO"
	CommandSet  Command = "SET"
	CommandGet  Command = "GET"
)

type Request struct {
	cmd  Command
	args [][]byte
}

type Store struct {
	mu   *sync.RWMutex
	data map[string]Value
}

func (s *Store) set(key string, value []byte, expires int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = Value{data: value, expires: expires}
}

func (s *Store) get(key string) []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.data[key].data
}

func (s *Store) gc() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, v := range s.data {
		if v.expires > 0 && v.expires < time.Now().UnixMilli() {
			delete(s.data, k)
		}
	}
}

type Value struct {
	data    []byte
	expires int64
}

var store = Store{
	mu:   &sync.RWMutex{},
	data: map[string]Value{},
}

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	go store.gc()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Failed to accept connection")
			os.Exit(1)
		}

		go handleConnection(conn)
	}

}

func handleConnection(conn net.Conn) {
	for {
		req, err := parseRequest(conn)
		if err != nil {
			fmt.Println("Error parsing request: ", err.Error())
			conn.Write([]byte("-ERR invalid request\r\n"))
			continue
		}

		var resp []byte
		switch req.cmd {
		case CommandPing:
			resp = []byte("+PONG\r\n")
		case CommandEcho:
			msg := append([]byte("+"), req.args[1]...)
			resp = []byte(string(msg) + "\r\n")
		case CommandSet:
			var expires int64
			for i, arg := range req.args[2:] {
				switch strings.ToUpper(string(arg)) {
				case "XP":
					expires, err = strconv.ParseInt(string(req.args[i+1]), 10, 64)
					if err != nil {
						resp = []byte("-ERR invalid expires\r\n")
					}
					break
				default:
					resp = []byte("-ERR invalid argument\r\n")
				}
			}

			if len(resp) != 0 {
				break
			}

			store.set(string(req.args[1]), req.args[2], expires)
			resp = []byte("+OK\r\n")
		case CommandGet:
			val := store.get(string(req.args[1]))
			if val == nil {
				resp = []byte("$-1\r\n")
			} else {
				resp = []byte("$" + strconv.Itoa(len(val)) + "\r\n" + string(val) + "\r\n")
			}
		default:
			resp = []byte("-ERR invalid request\r\n")
		}

		// send redis PONG to client
		n, err := conn.Write(resp)
		if err != nil {
			fmt.Println("Error writing to socket: ", err.Error())
			os.Exit(1)
		}

		fmt.Printf("Wrote %d bytes to socket\n", n)
	}
}

func parseRequest(r io.Reader) (Request, error) {
	req := Request{}
	scanner := bufio.NewScanner(r)
	if !scanner.Scan() {
		return req, fmt.Errorf("empty request")
	}

	arrDecl := scanner.Text()
	if arrDecl[0] != '*' {
		return req, fmt.Errorf("Expected *, got %s", arrDecl)
	}

	numArgs, err := strconv.Atoi(arrDecl[1:])
	if err != nil {
		return req, fmt.Errorf("Error parsing number of arguments: %s", err.Error())
	}

	for i := 0; i < numArgs; i++ {
		if !scanner.Scan() {
			return req, fmt.Errorf("Error reading argument %d", i)
		}

		strLenDecl := scanner.Text()
		if strLenDecl[0] != '$' {
			return req, fmt.Errorf("Expected $, got %s", strLenDecl)
		}

		strLen, err := strconv.Atoi(strLenDecl[1:])
		if err != nil {
			return req, fmt.Errorf("Error parsing string length: %s", err.Error())
		}

		if !scanner.Scan() {
			return req, fmt.Errorf("Error reading argument %d", i)
		}

		arg := scanner.Bytes()
		if len(arg) != strLen {
			return req, fmt.Errorf("Invalid string length for argument %d", i)
		}

		req.args = append(req.args, arg)
	}

	if err := scanner.Err(); err != nil {
		return req, fmt.Errorf("Error reading request: %s", err.Error())
	}

	switch strings.ToUpper(string(req.args[0])) {
	case "PING":
		req.cmd = CommandPing
	case "ECHO":
		req.cmd = CommandEcho
	case "SET":
		req.cmd = CommandSet
	case "GET":
		req.cmd = CommandGet
	default:
		return req, fmt.Errorf("Invalid command: %s", req.args[0])
	}

	return req, nil
}
