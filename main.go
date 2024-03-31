package main

import (
	"fmt"
	"net"

	"github.com/babajikomali/Distributed_KV_Store/constants"
	"github.com/babajikomali/Distributed_KV_Store/coordinator"
)

func main() {
	listener, err := net.Listen("tcp", ":"+constants.PORT)
	if err != nil {
		fmt.Println("Error listening:", err)
		return
	}
	defer listener.Close()

	fmt.Println("Server is listening on port 6969...")

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			return
		}

		go coordinator.Coordinator(conn)
	}
}
