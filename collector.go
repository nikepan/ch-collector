package ch_collector

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"log/slog"
	"sync"
	"sync/atomic"
)

type UniRow []interface{}

type ChCollector struct {
	limit     int
	items     []UniRow
	mu        sync.Mutex
	ch        chan UniRow
	tableName string
	started   atomic.Bool
	conn      driver.Conn
}

func NewCollector(conn driver.Conn, tableName string, limit int) *ChCollector {
	c := new(ChCollector)
	c.items = make([]UniRow, 0)
	c.ch = make(chan UniRow, 1000)
	c.tableName = tableName
	c.limit = limit
	c.conn = conn
	return c
}

func (c *ChCollector) Start() {
	c.CheckRunWorker()
}

func (c *ChCollector) Stop() error {
	close(c.ch)
	err := c.Send()
	errClose := c.conn.Close()
	if err != nil {
		return err
	}
	return errClose
}

func (c *ChCollector) Add(row UniRow) error {
	c.ch <- row
	c.CheckRunWorker()
	return nil
}

func (c *ChCollector) AddRow(row UniRow) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items = append(c.items, row)
	return len(c.items)
}

func (c *ChCollector) Count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}

func (c *ChCollector) PopItems() []UniRow {
	c.mu.Lock()
	defer c.mu.Unlock()
	items := c.items
	c.items = make([]UniRow, 0)
	return items
}

func (c *ChCollector) Send() error {
	items := c.PopItems()
	if len(items) == 0 {
		return nil
	}

	ctx := context.Background()
	batch, err := c.conn.PrepareBatch(ctx, "INSERT INTO "+c.tableName)
	if err != nil {
		return fmt.Errorf("prepare error: %s", err)
	}
	for _, item := range items {
		err := batch.Append(item...)
		if err != nil {
			return fmt.Errorf("append error: %s", err)
		}
	}
	err = batch.Send()
	if err != nil {
		slog.Info("items count", len(items))
		return fmt.Errorf("send error: %s", err)
	}
	return nil
}

func (c *ChCollector) Worker() {
	for {
		r, ok := <-c.ch
		if !ok {
			break
		}
		cnt := c.AddRow(r)
		if cnt > c.limit {
			err := c.Send()
			if err != nil {
				slog.Error("send", "err", err)
			}
		}
	}
}

func (c *ChCollector) CheckRunWorker() {
	ok := c.started.CompareAndSwap(false, true)
	if ok {
		go c.Worker()
	}
}
