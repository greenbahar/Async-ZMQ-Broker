package main

import (
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
	broker_storage "windows-old/interview/graph-broker/persistence/file/broker-storage"
)

var (
	inMemoryStorage                    = make(map[int64]string)
	NumberOfSentMessagesFromSender     = 0
	NumberOfReceivedMessagesInReceiver = 0
	mu                                 = sync.Mutex{}
)

func main() {
	context, _ := zmq.NewContext()

	// communicate to sender module (to "receive" data from sender module)
	receiver, _ := context.NewSocket(zmq.REP)
	receiver.Bind("tcp://*:5556")
	defer receiver.Close()

	// communicate to receiver-storage module (to "send" data to receiver-storage module)
	sender, _ := context.NewSocket(zmq.REQ)
	defer sender.Close()
	sender.Bind("tcp://*:5557")

	//StoreDataToFile()
	logNumberOfTransferredMessages()

	w := sync.WaitGroup{}

	// receive from sender server and store to file
	w.Add(1)
	go func() {
		defer w.Done()
		for {
			rawMessage, rcvErr := receiver.Recv(0)
			if rcvErr == nil {
				NumberOfSentMessagesFromSender++
				receiver.Send("received", 0)

				data := strings.Split(rawMessage, " ")
				messageID, _ := strconv.ParseInt(data[1], 10, 64)
				message := data[2]
				mu.Lock()
				inMemoryStorage[messageID] = message
				mu.Unlock()
			}
		}
	}()

	var lastSentID int64 = 0

	// sent to receiver and remove from the file
	w.Add(1)

	go func() {
		defer w.Done()
		for {
			//TODO
			//get data from file to send

			// send the data to receiver
			mu.Lock()
			sender.Send(inMemoryStorage[lastSentID], 0)
			mu.Unlock()
			_, rcvERR := sender.Recv(0)
			if rcvERR != nil {
				continue
			} else {
				mu.Lock()
				delete(inMemoryStorage, lastSentID)
				mu.Unlock()
				NumberOfReceivedMessagesInReceiver++
				lastSentID++
			}
		}
	}()

	w.Wait()
}

func loadDataFromFileStorage() (map[int64]string, error) {
	return broker_storage.ReadFileContent("./persistence/file/broker-storage/fileStorage.txt")
}

func StoreDataToFile() {
	// Persist data to file every 2 seconds for example
	ticker := time.NewTicker(2 * time.Second)
	brokerStorage := broker_storage.NewFileStorage()

	go func() {
		for {
			select {
			case <-ticker.C:
				mu.Lock()
				err := brokerStorage.AppendToFile("./persistence/file/broker-storage/fileStorage.txt", inMemoryStorage)
				mu.Unlock()
				if err != nil {
					log.Println(err)
				}
			}
		}
	}()
}

func logNumberOfTransferredMessages() {
	mu := sync.Mutex{}
	go func() {
		for {
			time.Sleep(time.Second)
			mu.Lock()
			fmt.Println("number of sent messages from Sender server: ", NumberOfSentMessagesFromSender, "******", "number of received messages i Receiver: ", NumberOfReceivedMessagesInReceiver)
			mu.Unlock()
		}
	}()
}
