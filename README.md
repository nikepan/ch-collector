# Clickhouse collector library

Very simple library to collect data and send with batches

## Usage

```go
c := NewCollector(conn, "my_table", 10000)
c.Start()
for i := 0; i < 100_000; i++ {
    c.Add([]interface{}{i, "test", 1.0})
}	
c.Stop()  // flush buffer and stop worker
```