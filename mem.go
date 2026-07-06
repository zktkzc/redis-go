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
	EventCmd                      = iota
	EventCheckBlockingListTimeout = iota
)

type BlockingListStatus struct {
	data    chan []byte
	start   int64
	timeout int64
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
	store map[string]Item
	t     *time.Ticker
	ch    chan Event
}

func NewStore(ch chan Event) *Store {
	return &Store{
		store: map[string]Item{},
		t:     time.NewTicker(1 * time.Second),
		ch:    ch,
	}
}

type BlockableList struct {
	key             string
	list            deque.Deque[any]
	blockingClients []*Client
	close           chan int
	eventCh         chan Event
}

const (
	Close     = 0
	ListAdded = 1
)

func NewBlockableList(key string, eventCh chan Event) *BlockableList {
	bl := &BlockableList{
		key,
		deque.Deque[any]{},
		[]*Client{},
		make(chan int),
		eventCh,
	}

	go func() {
		t := time.NewTicker(50 * time.Millisecond)
	loop:
		for {
			select {
			case <-t.C:
				eventCh <- Event{
					Type: EventCheckBlockingListTimeout,
					data: key,
				}
			case <-bl.close:
				log.Printf("[INFO] Block list closed.")
				break loop
			}
		}
	}()

	return bl
}

func (s *Store) GetRawValue(key string) (any, string) {
	if val, ok := s.store[key]; !ok || val.ts > 0 && val.ts < time.Now().UnixMilli() {
		return nil, "none"
	} else {
		switch v := val.data.(type) {
		case string:
			return v, "string"
		case *BlockableList:
			return v, "list"
		default:
			panic(fmt.Sprintf("[ERROR] Unsupported internal type: %T", val.data))
		}
	}
}

func (s *Store) NonBlockingLPOP(key string) (RESP, bool) {
	val, _ := s.GetRawValue(key)
	if val == nil {
		return nil, false
	}
	cur := s.store[key].data.(*BlockableList)
	if cur.list.Len() == 0 {
		return nil, false
	}
	return cur.list.PopFront().(RESP), true
}

func (s *Store) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Printf("[INFO] Loop cancelled")
		case <-s.t.C:
			log.Printf("[INFO] Store timely work")
		case ev, ok := <-s.ch:
			if !ok {
				log.Printf("[ERROR] Event channel closed")
			}

			log.Printf("[INFO] Event: %v", ev)
			err := s.HandleEvent(ev)
			if err != nil {
				log.Printf("[ERROR] Error handling event: %v", err.Error())
				os.Exit(1)
			}
		}
	}
}

func (s *Store) HandleEvent(ev Event) error {
	switch ev.Type {
	case EventCheckBlockingListTimeout:
		key := ev.data.(string)
		cur := s.store[key].data.(*BlockableList)
		next := []*Client{}
		for i, c := range cur.blockingClients {
			s := c.status.(BlockingListStatus)
			if time.Now().UnixMilli()-s.start >= s.timeout {
				log.Printf("[INFO] Client removed: %d", i)
				s.data <- nullArray
			} else {
				next = append(next, c)
			}
		}
		cur.blockingClients = next
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
				if val, _ := s.GetRawValue(key); val == nil {
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
							panic(fmt.Sprintf("[ERROR] Unknown expiry: %v", ex))
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
				val, _ := s.GetRawValue(listKey)
				if val == nil {
					s.store[listKey] = Item{
						data: NewBlockableList(listKey, s.ch),
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

				next := []*Client{}
				for _, c := range cur.blockingClients {
					v, got := s.NonBlockingLPOP(c.blockingKey)
					if got {
						res := Array{[]RESP{
							BulkString{c.blockingKey},
							v,
						}}
						c.status.(BlockingListStatus).data <- res.Encode()
					} else {
						next = append(next, c)
					}
				}
				cur.blockingClients = next
				SettleClient(ev.client, listKey, Integer{int64(cur.list.Len())}.Encode())
			case "LPOP", "RPOP":
				listKey := msg.elements[1].(BulkString).content
				val, _ := s.GetRawValue(listKey)
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
				val, _ := s.GetRawValue(listKey)
				if val == nil {
					s.store[listKey] = Item{
						data: NewBlockableList(listKey, s.ch),
						ts:   -1,
					}
				}
				cur := s.store[listKey].data.(*BlockableList)
				var timeout float64 = 24 * 365 * 10 * 3600
				if len(msg.elements) >= 3 {
					timeout = ToFloat(msg.elements[2]) * 1000
				}
				if cur.list.Len() == 0 {
					blstatus := BlockingListStatus{
						data:    make(chan []byte),
						start:   time.Now().UnixMilli(),
						timeout: int64(timeout),
					}
					cur.blockingClients = append(cur.blockingClients, &Client{listKey, blstatus})
					SettleClient(ev.client, listKey, blstatus)
				} else {
					res := Array{[]RESP{
						BulkString{listKey},
					}}
					res.elements = append(res.elements, cur.list.PopFront().(RESP))
					SettleClient(ev.client, listKey, res.Encode())
				}
			case "LRANGE":
				listKey := msg.elements[1].(BulkString).content
				val, _ := s.GetRawValue(listKey)
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
				log.Printf("[INFO] LRANGE: [%d, %d]", start, end)

				res := Array{
					elements: make([]RESP, end-start+1),
				}

				for i := start; i <= end; i++ {
					res.elements[i-start] = cur.list.At(i).(RESP)
				}
				SettleClient(ev.client, listKey, res.Encode())
			case "LLEN":
				listKey := msg.elements[1].(BulkString).content
				val, _ := s.GetRawValue(listKey)
				res := Integer{0}
				if val != nil {
					cur := s.store[listKey].data.(*BlockableList)
					res.content = int64(cur.list.Len())
				}
				SettleClient(ev.client, listKey, res.Encode())
			case "TYPE":
				key := msg.elements[1].(BulkString).content
				_, t := s.GetRawValue(key)
				SettleClient(ev.client, "", []byte("+"+t+"\r\n"))
			default:
				panic(fmt.Sprintf("[ERROR] Unknown command: %v", cmd.content))
			}
		} else {
			panic("[ERROR] Command should be a bulk string")
		}
	default:
		return fmt.Errorf("[ERROR] Unknown event: %v", ev)
	}
	return nil
}

func ToFloat(v RESP) float64 {
	switch raw := v.(type) {
	case BulkString:
		val, err := strconv.ParseFloat(raw.content, 64)
		if err != nil {
			panic(fmt.Sprintf("[ERROR] Error parsing value: %v", v))
		}
		return val
	default:
		panic(fmt.Sprintf("[ERROR] Cannot parsing value: %v", v))
	}
}

func ToInt(v RESP) int {
	switch raw := v.(type) {
	case BulkString:
		val, err := strconv.Atoi(raw.content)
		if err != nil {
			panic(fmt.Sprintf("[ERROR] Error parsing value: %v", v))
		}
		return val
	case Integer:
		return int(raw.content)
	default:
		panic(fmt.Sprintf("[ERROR] Cannot parsing value: %v", v))
	}
}
