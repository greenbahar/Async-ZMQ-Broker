package main

import (
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"math/rand"
	"os"
)

func main() {
	context, _ := zmq.NewContext()
	socket, _ := context.NewSocket(zmq.REQ)
	defer socket.Close()
	if err := socket.Connect("tcp://localhost:5556"); err != nil {
		panic(err)
	}

	random8kFile, err := os.ReadFile("utils/randomGenerator/random8kFile.txt")
	if err != nil {
		panic(err)
	}

	id := 0
	for {
		messageSize := rand.Intn(8000) + 50
		message := fmt.Sprintf("id: %d %s", id, string(random8kFile[:messageSize]))
		socket.Send(message, 0)
		socket.Recv(0)
		id++
	}
}
