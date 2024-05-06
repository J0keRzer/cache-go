package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

type Aof struct {
	file *os.File
	rd   *bufio.Reader
	mu   sync.Mutex
}

func NewAof(path string) (*Aof, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	aof := &Aof{
		file: f,
		rd:   bufio.NewReader(f),
	}

	go func() {
		for {
			aof.mu.Lock()
			aof.file.Sync()
			aof.mu.Unlock()
			time.Sleep(time.Second)
		}
	}()

	return aof, nil
}

func (aof *Aof) Close() error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	return aof.file.Close()
}

func (aof *Aof) Write(value Value) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	_, err := aof.file.Write(value.Marshal())
	return err
}

func (aof *Aof) Read(load func(Value)) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	// check if file is present
	if _, err := os.Stat("./database.aof"); os.IsNotExist(err) {
		return nil
	}

	var filedata []byte
	for {
		b, err := aof.rd.ReadByte()
		if b == '\x00' { // EOF
			break
		}
		if err != nil {
			fmt.Println("Error while reading aof file: ", err.Error())
		}

		filedata = append(filedata, b)
	}

	// get each prompt
	var matches []string

	last := 0
	for i := range string(filedata) {
		if string(filedata[i]) == "*" || i == len(filedata)-1 {
			matches = append(matches, string(filedata[last:i]))
			last = i
		}
	}
	if len(matches) > 0 {
		matches = matches[1:]
	}

	// convert resp into Value object
	for i, val := range matches {

		value, err := NewResp(strings.NewReader(val)).Read()
		if err != nil {
			fmt.Println("Error", err.Error(), " cannot load record ", i)
			return err
		}

		load(value)
	}
	return nil
}
