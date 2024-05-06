package main

import (
	"fmt"
	"net"
	"os"
	"strings"
)

func main() {

	// Craete db file or read if it exists
	aof, err := NewAof("database.aof")
	if err != nil {
		fmt.Println("Error loading database: ", err.Error())
		return
	}
	defer aof.Close()

	// Read data in saved file
	aof.Read(func(value Value) {
		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]

		handler, ok := Handlers[command]
		if !ok {
			fmt.Println("Invalid command: ", command)
			return
		}

		handler(args)
	})

	// create tcp server
	l, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println(err)
		return
	}

	conn, err := l.Accept()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer conn.Close()

	for {
		// init redis protocol reader
		resp := NewResp(conn)

		// read user's input
		value, err := resp.Read()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// checking if input is a command

		if value.typ != "array" {
			fmt.Println("Invalid request, expected array")
			continue
		}

		if len(value.array) == 0 {
			fmt.Println("Invalid request, expected array length > 0")
			continue
		}

		// first string in Values array is command
		command := strings.ToUpper(value.array[0].bulk)
		// the rest of values are arguments to the given command
		args := value.array[1:]

		writer := NewWriter(conn)

		handler, ok := Handlers[command]
		if !ok {
			fmt.Println("Invalid command: ", command)
			writer.Write(Value{typ: "string", str: ""})
			continue
		}

		// save to local file
		if command == "SET" || command == "HSET" {
			aof.Write(value)
		}

		// command result
		result := handler(args)
		// write to static db file
		writer.Write(result)
	}
}
