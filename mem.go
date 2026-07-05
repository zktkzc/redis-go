package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gammazero/deque"
)

type EventType int

const (
	EventCmd = iota
)

type BlockingListStatus struct {
	data chan []byte
}

type Client struct {
	blockingKey string
	status      any
}

type Event struct {
	Type   EventType
	data   any
	client chan Client
}

type Item struct {
	data any
	ts   int64 // 时间戳，毫秒
}

type Store struct {
	store           map[string]Item
	t               *time.Ticker
	blockingClients map[string][]*Client
}

func NewStore() *Store {
	return &Store{
		store:           make(map[string]Item),
		t:               time.NewTicker(1 * time.Second),
		blockingClients: make(map[string][]*Client),
	}
}

type BlockableList struct {
	list deque.Deque[any]
}

func NewBlockableList() *BlockableList {
	return &BlockableList{deque.Deque[any]{}}
}

func (s *Store) GetRawValue(key string) any {
	if val, ok := s.store[key]; !ok || val.ts > 0 && val.ts < time.Now().UnixMilli() {
		return nil
	} else {
		switch v := val.data.(type) {
		case string:
			return v
		case *BlockableList:
			return v
		default:
			panic(fmt.Sprintf("Unknown internal type: %v", val.data))
		}
	}
}

func (s *Store) NonBlockingLPOP(key string) (RESP, bool) {
	val := s.GetRawValue(key)
	if val == nil {
		return nil, false
	}
	cur := s.store[key].data.(*BlockableList)
	if cur.list.Len() == 0 {
		return nil, false
	}
	return cur.list.PopFront().(RESP), true
}

func (s *Store) Start(ctx context.Context, ch <-chan Event) {
	for {
		select {
		case <-ctx.Done():
			log.Printf("loop cancelled")
		case <-s.t.C:
			log.Printf("store timely work")
			for k, clients := range s.blockingClients {
				log.Printf("Processing blocking key %v...", k)
				for _, c := range clients {
					v, got := s.NonBlockingLPOP(c.blockingKey)
					if !got {
						break
					}
					res := Array{[]RESP{
						BulkString{k},
						v,
					}}
					c.status.(BlockingListStatus).data <- res.Encode()
				}
			}
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
		msg := ev.data.(Array)
		if cmd, ok := msg.elements[0].(BulkString); ok {
			switch command := strings.ToUpper(cmd.content); command {
			case "PING":
				SettleClient(ev.client, "", []byte("+PONG\r\n"))
			case "ECHO":
				key := msg.elements[1].(BulkString)
				SettleClient(ev.client, key.content, key.Encode())
			case "GET":
				key := msg.elements[1].(BulkString).content
				if val := s.GetRawValue(key); val == nil {
					SettleClient(ev.client, key, nullBulkString)
				} else {
					bv := BulkString{val.(string)}
					SettleClient(ev.client, key, bv.Encode())
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
				SettleClient(ev.client, key, OK)
			case "RPUSH", "LPUSH":
				listKey := msg.elements[1].(BulkString).content
				val := s.GetRawValue(listKey)
				if val == nil {
					s.store[listKey] = Item{
						data: NewBlockableList(),
						ts:   -1,
					}
				}
				values := msg.elements[2:]
				cur := s.store[listKey].data.(*BlockableList)
				for _, v := range values {
					if command == "RPUSH" {
						cur.list.PushBack(v)
					} else {
						cur.list.PushFront(v)
					}
				}
				s.store[listKey] = Item{
					data: cur,
					ts:   -1,
				}
				SettleClient(ev.client, listKey, Integer{int64(cur.list.Len())}.Encode())
			case "LPOP", "RPOP":
				listKey := msg.elements[1].(BulkString).content
				val := s.GetRawValue(listKey)
				if val == nil {
					SettleClient(ev.client, listKey, nullBulkString)
					return nil
				}
				cur := s.store[listKey].data.(*BlockableList)
				if cur.list.Len() == 0 {
					SettleClient(ev.client, listKey, nullBulkString)
					return nil
				}
				num := 1
				res := Array{[]RESP{}}
				isArray := false
				if len(msg.elements) >= 3 {
					num = ToInt(msg.elements[2])
					isArray = true
				}
				for num > 0 {
					if cur.list.Len() == 0 {
						break
					}
					if command == "RPOP" {
						res.elements = append(res.elements, cur.list.PopBack().(RESP))
					} else {
						res.elements = append(res.elements, cur.list.PopFront().(RESP))
					}
					num -= 1
				}
				if isArray {
					SettleClient(ev.client, listKey, res.Encode())
				} else {
					SettleClient(ev.client, listKey, res.elements[0].Encode())
				}
			case "BLPOP":
				listKey := msg.elements[1].(BulkString).content
				val := s.GetRawValue(listKey)
				if val == nil {
					s.store[listKey] = Item{
						data: NewBlockableList(),
						ts:   -1,
					}
				}
				cur := s.store[listKey].data.(*BlockableList)
				if cur.list.Len() == 0 {
					blstatus := BlockingListStatus{
						data: make(chan []byte),
					}
					s.blockingClients[listKey] = append(s.blockingClients[listKey], &Client{listKey, blstatus})
					SettleClient(ev.client, listKey, blstatus)
				} else {
					res := cur.list.PopFront().(RESP).Encode()
					SettleClient(ev.client, listKey, res)
				}
			case "LRANGE":
				listKey := msg.elements[1].(BulkString).content
				val := s.GetRawValue(listKey)
				if val == nil {
					SettleClient(ev.client, listKey, Array{}.Encode())
					return nil
				}
				cur := s.store[listKey].data.(*BlockableList)

				start := ToInt(msg.elements[2])
				if start < 0 {
					start = max(start+cur.list.Len(), 0)
				}
				end := ToInt(msg.elements[3])
				if end < 0 {
					end = max(end+cur.list.Len(), 0)
				}
				end = min(end, cur.list.Len()-1)
				log.Printf("LRANGE: [%d, %d]", start, end)

				res := Array{
					elements: make([]RESP, end-start+1),
				}

				for i := start; i <= end; i++ {
					res.elements[i-start] = cur.list.At(i).(RESP)
				}
				SettleClient(ev.client, listKey, res.Encode())
			case "LLEN":
				listKey := msg.elements[1].(BulkString).content
				val := s.GetRawValue(listKey)
				res := Integer{0}
				if val != nil {
					cur := s.store[listKey].data.(*BlockableList)
					res.content = int64(cur.list.Len())
				}
				SettleClient(ev.client, listKey, res.Encode())
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
