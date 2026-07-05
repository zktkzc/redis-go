package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type EventType int

const (
	EventCmd = iota
)

type Event struct {
	Type EventType
	Data any
	conn net.Conn
}

type Item struct {
	data any
	ts   int64 // 时间戳，毫秒
}

type Store struct {
	store map[string]Item
	t     *time.Ticker
}

func NewStore() *Store {
	return &Store{
		store: make(map[string]Item),
		t:     time.NewTicker(1 * time.Second),
	}
}

func (s *Store) GetRawValue(key string) any {
	if val, ok := s.store[key]; !ok || val.ts > 0 && val.ts < time.Now().UnixMilli() {
		return nil
	} else {
		switch v := val.data.(type) {
		case string:
			return v
		case []any:
			return v
		default:
			panic(fmt.Sprintf("Unknown internal type: %v", val.data))
		}
	}
}

func (s *Store) Start(ctx context.Context, ch <-chan Event) {
	for {
		select {
		case <-ctx.Done():
			log.Printf("loop cancelled")
		case <-s.t.C:
			log.Printf("store timely work")
		case ev, ok := <-ch:
			if !ok {
				log.Printf("event channel closed")
			}

			log.Printf("event: %v", ev)
			err := s.HandleEvent(ev)
			if err != nil {
				log.Printf("Error handling event: %v", err.Error())
				os.Exit(1)
			}
		}
	}
}

func (s *Store) HandleEvent(ev Event) error {
	switch ev.Type {
	case EventCmd:
		msg := ev.Data.(Array)
		if cmd, ok := msg.elements[0].(BulkString); ok {
			switch strings.ToUpper(cmd.content) {
			case "PING":
				WriteWithBail(ev.conn, []byte("+PONG\r\n"))
			case "ECHO":
				key := msg.elements[1].(BulkString)
				WriteWithBail(ev.conn, key.Encode())
			case "GET":
				key := msg.elements[1].(BulkString).content
				if val := s.GetRawValue(key); val == nil {
					WriteWithBail(ev.conn, nullBulkString)
				} else {
					bv := BulkString{val.(string)}
					WriteWithBail(ev.conn, bv.Encode())
				}
			case "SET":
				key := msg.elements[1].(BulkString).content
				value := msg.elements[2].(BulkString).content
				var expired int64 = -1
				if len(msg.elements) > 3 {
					if ex, ok := msg.elements[3].(BulkString); ok {
						t := ToInt(msg.elements[4])
						ex := strings.ToUpper(ex.content)
						switch ex {
						case "EX":
							expired = time.Now().Add(time.Duration(t) * time.Second).UnixMilli()
						case "PX":
							expired = time.Now().Add(time.Duration(t) * time.Millisecond).UnixMilli()
						default:
							panic(fmt.Sprintf("Unknown expiry: %v", ex))
						}
					}
				}
				s.store[key] = Item{
					data: value,
					ts:   expired,
				}
				WriteWithBail(ev.conn, OK)
			case "RPUSH":
				listKey := msg.elements[1].(BulkString).content
				val := s.GetRawValue(listKey)
				if val == nil {
					s.store[listKey] = Item{
						data: make([]any, 0, 5),
						ts:   -1,
					}
				}
				values := msg.elements[2:]
				cur := s.store[listKey].data.([]any)
				for _, v := range values {
					cur = append(cur, v)
				}
				s.store[listKey] = Item{
					data: cur,
					ts:   -1,
				}
				WriteWithBail(ev.conn, Integer{int64(len(cur))}.Encode())
			case "LRANGE":
				listKey := msg.elements[1].(BulkString).content
				val := s.GetRawValue(listKey)
				if val == nil {
					WriteWithBail(ev.conn, Array{}.Encode())
					return nil
				}
				cur := s.store[listKey].data.([]any)

				start := ToInt(msg.elements[2])
				if start < 0 {
					start = max(start+len(cur), 0)
				}
				end := ToInt(msg.elements[3])
				if end < 0 {
					end = max(end+len(cur), 0)
				}
				end = min(end, len(cur)-1)
				log.Printf("LRANGE: [%d, %d]", start, end)

				res := Array{
					elements: make([]RESP, end-start+1),
				}

				for i := start; i <= end; i++ {
					res.elements[i-start] = cur[i].(RESP)
				}
				WriteWithBail(ev.conn, res.Encode())
			default:
				panic(fmt.Sprintf("Unknown command: %v", cmd.content))
			}
		} else {
			panic("Command should be a bulk string")
		}
	default:
		return fmt.Errorf("unknown event: %v", ev)
	}
	return nil
}

func ToInt(v RESP) int {
	switch raw := v.(type) {
	case BulkString:
		val, err := strconv.Atoi(raw.content)
		if err != nil {
			panic(fmt.Sprintf("Error parsing value: %v", v))
		}
		return val
	case Integer:
		return int(raw.content)
	default:
		panic(fmt.Sprintf("Cannot parsing value: %v", v))
	}
}
