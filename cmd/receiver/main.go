package main

import (
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"time"
)

var lastReceivedMessageID int64

func main() {
	context, _ := zmq.NewContext()

	socket, _ := context.NewSocket(zmq.REP)
	defer socket.Close()
	socket.Connect("tcp://localhost:5557")

	logNumberOfReceivedMessages()

	for {
		socket.Recv(0)
		socket.Send("ok", 0)
		lastReceivedMessageID++
	}
}

func logNumberOfReceivedMessages() {
	go func() {
		for {
			time.Sleep(time.Second)
			fmt.Println("last received messages ID: ", lastReceivedMessageID)
		}
	}()
}
