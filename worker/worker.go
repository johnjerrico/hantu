package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/go-memdb"
	"github.com/johnjerrico/hantu/schema"
	"github.com/korovkin/limiter"
)

type Command func(ctx context.Context, request any)

type Worker interface {
	Start()
	Stop()
	Register(name string, cmd Command)
}

func New(domain, id string, max int, interval time.Duration, inmem *memdb.MemDB) Worker {
	return &worker{
		max:      max,
		interval: interval,
		commands: make(map[string]Command),
		exit:     make(chan byte),
		inmem:    inmem,
	}
}

type worker struct {
	max      int
	interval time.Duration
	commands map[string]Command
	exit     chan byte
	inmem    *memdb.MemDB
	mu       sync.Mutex
}

func (w *worker) Register(name string, cmd Command) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.commands[name] == nil {
		w.commands[name] = cmd
	}
}

func (w *worker) Start() {
	go w.spawn()
}

func (w *worker) Stop() {
	tx := w.inmem.Snapshot().Txn(false)
	it, _ := tx.Get("job", "id")
	for obj := it.Next(); obj != nil; obj = it.Next() {
		current := obj.(*schema.Job)
		writeTx := w.inmem.Txn(true)
		writeTx.Delete("job", current)
		writeTx.Commit()
	}
	tx.Abort()
	w.exit <- byte('1')

}

func (w *worker) spawn() {
	var c *limiter.ConcurrencyLimiter
	for {
		select {
		case <-w.exit:
			return
		default:
			c = limiter.NewConcurrencyLimiter(w.max)
			tx := w.inmem.Snapshot().Txn(false)
			it, _ := tx.Get("job", "id")
			for obj := it.Next(); obj != nil; obj = it.Next() {
				current := obj.(*schema.Job)
				writeTx := w.inmem.Txn(true)
				writeTx.Delete("job", current)
				writeTx.Commit()
				c.Execute(func() {
					w.mu.Lock()
					defer w.mu.Unlock()
					if w.commands[current.Name] != nil {
						defer func() {
							if r := recover(); r != nil {
								fmt.Println("program recover from panic")
							}
						}()
						if current.Delay > 0 {
							time.Sleep(current.Delay)
						}
						w.commands[current.Name](current.Ctx, current.Request)
					}
				})
			}
			c.WaitAndClose()
			tx.Abort()
		}
		time.Sleep(time.Millisecond)
	}
}
