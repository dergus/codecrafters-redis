package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

type Command string

const (
	CommandPing Command = "PING"
	CommandEcho Command = "ECHO"
)

type Request struct {
	cmd  Command
	args []string
}

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

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
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println("Error reading from connection: ", err.Error())
			os.Exit(1)
		}

		str := string(buf[:n])
		fmt.Println("Received: ", str)

		req, err := parseRequest(str)
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
			resp = []byte("+" + strings.Join(req.args, " ") + "\r\n")
		default:
			resp = []byte("-ERR invalid request\r\n")
		}

		// send redis PONG to client
		n, err = conn.Write(resp)
		if err != nil {
			fmt.Println("Error writing to socket: ", err.Error())
			os.Exit(1)
		}

		fmt.Printf("Wrote %d bytes to socket\n", n)
	}
}

func parseRequest(str string) (Request, error) {
	req := Request{}
	parts := strings.Split(str, "\r\n")
	if len(parts) < 2 {
		return req, fmt.Errorf("Invalid request: %s", str)
	}

	// parse command
	if parts[0][0] != '*' {
		return req, fmt.Errorf("Invalid request: %s", str)
	}

	countArgs, err := strconv.Atoi(parts[0][1:])
	if err != nil {
		return req, fmt.Errorf("Invalid request: %s", str)
	}

	if len(parts) != countArgs+1 {
		return req, fmt.Errorf("Invalid request: %s", str)
	}

	for _, p := range parts[1:] {
		if p[0] != '$' {
			return req, fmt.Errorf("Invalid request: %s", str)
		}

		countBytes, err := strconv.Atoi(p[1:])
		if err != nil {
			return req, fmt.Errorf("Invalid request: %s", str)
		}

		if len(parts) != countBytes+1 {
			return req, fmt.Errorf("Invalid request: %s", str)
		}

		req.args = append(req.args, parts[1])
	}

	switch strings.ToUpper(req.args[0]) {
	case "PING":
		req.cmd = CommandPing
	case "ECHO":
		req.cmd = CommandEcho
	default:
		return req, fmt.Errorf("Invalid request: %s", str)
	}

	return req, nil
}
