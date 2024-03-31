package coordinator

import (
	"fmt"
	"net"
	"strings"
)

func verifyArgs(commands []string) string {
	var response string

	if commands[0] != "get" && commands[0] != "set" &&
		commands[0] != "put" && commands[0] != "del" {
		response = fmt.Sprintf("command %s is invalid\n", commands[0])
	} else if commands[0] == "get" && (len(commands) != 2 || commands[1] == "") {
		response = fmt.Sprintln("command is invalid")
	} else if commands[0] == "set" && (len(commands) != 3 || commands[1] == "") {
		response = fmt.Sprintln("command is invalid")
	} else if commands[0] == "put" && (len(commands) != 2 || commands[1] == "") {
		response = fmt.Sprintln("command is invalid")
	} else if commands[0] == "del" && (len(commands) != 2 || commands[1] == "") {
		response = fmt.Sprintln("command is invalid")
	} else {
		response = ""
	}
	return response
}

func Coordinator(conn net.Conn) {
	defer conn.Close()

	buffer := make([]byte, 1024)
	n, err := conn.Read(buffer)
	if err != nil {
		fmt.Println("Error reading:", err)
		return
	}

	fmt.Printf("Received data: %s\n", buffer[:n])

	trimmedInput := strings.TrimSpace(string(buffer[:n]))

	commands := strings.Split(trimmedInput, " ")

	response := verifyArgs(commands)

	if response != "" {
		_, err = conn.Write([]byte(response))
		if err != nil {
			fmt.Println("Error writing:", err)
			return
		}
	}

	
}
