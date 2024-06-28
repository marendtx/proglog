package server

import (
	"fmt"
	"sync"
)

type Log struct {
	mu      sync.Mutex // 並行プログラミングにおいて、排他制御を実現する構造体
	records []Record
}

func NewLog() *Log {
	return &Log{}
}

func (c *Log) Append(record Record) (uint64, error) {
	c.mu.Lock()         // 他のゴルーチンからの読み取り、書き込みをブロックする
	defer c.mu.Unlock() // ミューテックスを解放する。
	record.Offset = uint64(len(c.records))
	c.records = append(c.records, record)
	return record.Offset, nil
}

func (c *Log) Read(offset uint64) (Record, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if offset >= uint64(len(c.records)) {
		return Record{}, ErrOffsetNotFound
	}
	return c.records[offset], nil
}

type Record struct {
	Value  []byte `json:"value"`
	Offset uint64 `json:"offset"`
}

var ErrOffsetNotFound = fmt.Errorf("offset not found")
