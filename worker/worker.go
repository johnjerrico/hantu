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

type Command func(ctx context.Context, request interface{})

type Worker interface {
	Start()
	Stop()
	Register(name string, cmd Command)
}

func New(domain, id string, max int, interval time.Duration, inmem *memdb.MemDB /*, scheduler scheduler.Scheduler*/) Worker {
	return &worker{
		max:          max,
		interval:     interval,
		commands:     make(map[string]Command),
		exit:         make(chan byte),
		cancel_funcs: make(map[string]context.CancelFunc),
		inmem:        inmem,
	}
}

type worker struct {
	max          int
	interval     time.Duration
	commands     map[string]Command
	exit         chan byte
	cancel_funcs map[string]context.CancelFunc
	inmem        *memdb.MemDB
	mutex        sync.RWMutex
}

func (w *worker) Register(name string, cmd Command) {
	w.commands[name] = cmd
}

func (w *worker) Start() {
	go w.spawn()
}

func (w *worker) Stop() {

	for _, item := range w.cancel_funcs {
		if item != nil {
			item()
		}
	}
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
					if w.commands[current.Name] != nil {
						defer func() {
							if r := recover(); r != nil {
								fmt.Println("program recover from panic")
							}
						}()
						ctx, cancel_func := context.WithCancel(context.Background())
						w.mutex.Lock()
						w.cancel_funcs[current.Id] = cancel_func
						w.mutex.Unlock()
						w.commands[current.Name](ctx, current)
						w.mutex.Lock()
						w.cancel_funcs[current.Id] = nil
						w.mutex.Unlock()
					}
				})
			}
			c.WaitAndClose()
			tx.Abort()
		}
		time.Sleep(time.Millisecond)
	}
}
