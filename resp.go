package main

import (
	"bufio"
	"bytes"
	"fmt"
	"strconv"
)

type RESP interface {
	Encode() []byte
}

type Array struct {
	elements []RESP
}

type Integer struct {
	content int64
}

type BulkString struct {
	content string
}

type Entry struct {
	id    string
	key   string
	value string
}

type Stream struct {
	key     string
	entries []*Entry
}

type Decoder struct {
	s *bufio.Scanner
}

// 常量
var (
	nullBulkString = []byte("$-1\r\n")
	nullArray      = []byte("*-1\r\n")
	OK             = []byte("+OK\r\n")
)

func (bs BulkString) Encode() []byte {
	res := make([]byte, 0, len(bs.content)+1+10)
	res = append(res, '$')
	length := strconv.Itoa(len(bs.content))
	res = append(res, []byte(length)...)
	res = append(res, "\r\n"...)
	res = append(res, []byte(bs.content)...)
	res = append(res, "\r\n"...)
	return res
}

func (i Integer) Encode() []byte {
	res := make([]byte, 0)
	res = append(res, ':')
	value := strconv.Itoa(int(i.content))
	res = append(res, []byte(value)...)
	res = append(res, "\r\n"...)
	return res
}

func (arr Array) Encode() []byte {
	res := make([]byte, 0, 50)
	res = append(res, '*')
	length := strconv.Itoa(len(arr.elements))
	res = append(res, []byte(length)...)
	res = append(res, "\r\n"...)
	for _, a := range arr.elements {
		res = append(res, a.Encode()...)
	}
	return res
}

func (d *Decoder) Decode(data []byte) (RESP, error) {
	header, content, found := bytes.Cut(data, []byte{'\r', '\n'})
	if !found {
		panic(fmt.Sprintf("[ERROR] Failed to decode bulk string, there is no '\r\n' in %v", data))
	}

	t := header[0]
	switch t {
	case ':': // integers
		value, err := strconv.ParseInt(string(header[1:]), 10, 64)
		if err != nil {
			panic(fmt.Sprintf("[ERROR] Failed to decode bulk string: %v", err.Error()))
		}
		return Integer{value}, nil
	case '$': // bulk string
		length, err := strconv.ParseInt(string(header[1:]), 10, 64)
		if err != nil {
			panic(fmt.Sprintf("[ERROR] Failed to decode bulk string: %v", err.Error()))
		}
		return BulkString{string(content[:length])}, nil
	case '*':
		count, err := strconv.ParseInt(string(header[1:]), 10, 64)
		if err != nil {
			panic(fmt.Sprintf("[ERROR] Failed to decode bulk string: %v", err.Error()))
		}
		arr := Array{elements: make([]RESP, int(count))}
		for i := 0; i < int(count); i++ {
			if d.s.Scan() {
				arr.elements[i], err = d.Decode(d.s.Bytes())
				if err != nil {
					panic(fmt.Sprintf("[ERROR] Failed to decode array: %v", err.Error()))
				}
			} else {
				panic(fmt.Sprintf("[ERROR] Failed to decode array: %v", d.s.Err()))
			}
		}
		return arr, nil
	default:
		panic(fmt.Sprintf("[ERROR] Not supported: %v", t))
	}
}

func split(data []byte, _ bool) (advance int, token []byte, err error) {
	header, content, found := bytes.Cut(data, []byte{'\r', '\n'})
	if !found {
		return 0, nil, nil
	}

	t := header[0]
	switch t {
	case '$': // bulk string
		length, err := strconv.ParseInt(string(header[1:]), 10, 64)
		if err != nil {
			return 0, nil, err
		}
		if int64(len(content)) < length {
			return 0, nil, nil
		}
		totalLength := len(header) + 4 + int(length)
		return totalLength, data[:totalLength], nil
	case '*': // array
		_, err := strconv.ParseInt(string(header[1:]), 10, 64)
		if err != nil {
			return 0, nil, err
		}
		totalLength := len(header) + 2
		return totalLength, data[:totalLength], nil
	case ':': // integers
		totalLength := len(header) + 2
		return totalLength, data[:totalLength], nil
	default:
		panic(fmt.Sprintf("[ERROR] Not supported: %v", t))
	}
}
