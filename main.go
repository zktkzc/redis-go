package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"os"
)

func SettleClient(client chan Client, key string, status any) {
	cs := Client{key, status}
	client <- cs
}

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	fmt.Println("Redis server start on 0.0.0.0:6379...")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan Event)
	store := NewStore(events)
	go store.Start(ctx)

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}

		go func() {
			scanner := bufio.NewScanner(conn)
			scanner.Split(split)
			decoder := Decoder{s: scanner}
			respCh := make(chan Client)
			for scanner.Scan() {
				data := scanner.Bytes()
				log.Printf("[INFO] Received line(%v): %v", conn.RemoteAddr(), data)

				msg, err := decoder.Decode(data)
				if err != nil {
					log.Printf("[ERROR] Error decoding data(%v): %v", conn.RemoteAddr(), err)
					os.Exit(1)
				}

				switch msg := msg.(type) {
				case BulkString:
					log.Printf("[INFO] Message(bulk string): %v", msg)
				case Array:
					log.Printf("[INFO] Message(array): %#v", msg)
					events <- Event{Type: EventCmd, data: msg, client: respCh}
					resp := <-respCh
					status := resp.status
					switch s := status.(type) {
					case []byte:
						_, err := conn.Write(s)
						if err != nil {
							log.Printf("[ERROR] Error writing to connection %v: %v", conn.RemoteAddr(), err.Error())
							os.Exit(1)
						}
					case BlockingListStatus:
						data := <-s.data
						_, err := conn.Write(data)
						if err != nil {
							log.Printf("[ERROR] Error writing to connection %v: %v", conn.RemoteAddr(), err.Error())
							os.Exit(1)
						}
					}
				default:
					panic("[ERROR] Unknown message type")
				}
			}
			if err := scanner.Err(); err != nil {
				fmt.Println("[ERROR] Error reading from connection: ", err.Error())
				os.Exit(1)
			}
		}()
	}
}
