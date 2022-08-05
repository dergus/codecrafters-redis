package main

import (
	"fmt"
	"net"
	"os"
)

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
		_, err := conn.Read(nil)
		if err != nil {
			fmt.Println("Error reading from connection: ", err.Error())
			os.Exit(1)
		}

		// send redis PONG to client
		n, err := conn.Write([]byte("+PONG\r\n"))
		if err != nil {
			fmt.Println("Error writing to socket: ", err.Error())
			os.Exit(1)
		}

		fmt.Printf("Wrote %d bytes to socket\n", n)
	}
}
