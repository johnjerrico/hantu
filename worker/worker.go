package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/johnjerrico/hantu/scheduler"
	"github.com/johnjerrico/hantu/schema"

	"github.com/hashicorp/go-memdb"
)

type Command func(ctx context.Context, id string, request interface{}) error
type Checksum func(ctx context.Context, jobs []schema.Job) (shouldCancelled []string, err error)

type Worker interface {
	Start()
	Stop()
	RegisterCommand(name string, cmd Command)
	RegisterChecksum(name string, checksum Checksum)
}

func New(domain, id string, max int, interval time.Duration, inmem *memdb.MemDB, scheduler scheduler.Scheduler) Worker {
	return &worker{
		max:          max,
		interval:     interval,
		commands:     make(map[string]Command),
		checksums:    make(map[string]Checksum),
		queue:        make(chan *schema.Job, max),
		exit:         make(chan byte),
		cancel_funcs: make(map[string]context.CancelFunc),
		inmem:        inmem,
		scheduler:    scheduler,
	}
}

type worker struct {
	id           string
	domain       string
	max          int
	interval     time.Duration
	commands     map[string]Command
	checksums    map[string]Checksum
	queue        chan *schema.Job
	exit         chan byte
	cancel_funcs map[string]context.CancelFunc
	inmem        *memdb.MemDB
	scheduler    scheduler.Scheduler
}

func (w *worker) RegisterCommand(name string, cmd Command) {
	w.commands[name] = cmd
}

func (w *worker) RegisterChecksum(name string, checksum Checksum) {
	w.checksums[name] = checksum
}

func (w *worker) Start() {
	w.scheduler.Register(w.domain, w.id)
	w.checksum(w.interval)
	go w.spawn()
	go w.run()
}

func (w *worker) Stop() {
	close(w.queue)
	w.exit <- byte('1')
	for _, item := range w.cancel_funcs {
		item()
	}

}

func (w *worker) checksum(interval time.Duration) {
	if interval < 1 {
		return
	}
	_ticker := time.NewTicker(interval)
	go func() {
		for {
			select {
			case <-w.exit:
				return
			case _time := <-_ticker.C:
				if _time.Second() == 0 {
					continue
				}
				if err := w.scheduler.Active(); err == nil {
					tx := w.inmem.Snapshot().Txn(false)
					it, _ := tx.Get("job", "id")
					tobeProcessed := make(map[string][]schema.Job)
					index := make(map[string]schema.Job)
					for obj := it.Next(); obj != nil; obj = it.Next() {
						current := obj.(*schema.Job)
						if len(tobeProcessed[current.Checksum]) == 0 {
							tobeProcessed[current.Checksum] = make([]schema.Job, 0)
						}
						index[current.Id] = *current
						tobeProcessed[current.Checksum] = append(tobeProcessed[current.Checksum], *current)
					}
					tx.Abort()
					for checksum_func, pending := range tobeProcessed {
						shouldBeCancelled, err := w.checksums[checksum_func](
							context.Background(),
							pending,
						)
						if err == nil {
							writeTx := w.inmem.Txn(true)
							for _, job := range shouldBeCancelled {
								current := index[job]
								writeTx.Delete("job", current)
								writeTx.Commit()
								w.cancel_funcs[job]()
								w.cancel_funcs[job] = nil
							}
						} else {
							fmt.Println(err.Error())
						}
					}
					w.scheduler.Sleep()
				}
			}
		}
	}()
}

func (w *worker) spawn() {
	for {
		select {
		case <-w.exit:
			return
		default:
			n := len(w.queue)
			shouldrun := n < w.max
			if shouldrun {
				tx := w.inmem.Snapshot().Txn(false)
				it, _ := tx.Get("job", "id")
				for obj := it.Next(); obj != nil; obj = it.Next() {
					current := obj.(*schema.Job)
					copy := schema.Job{
						Id:               current.Id,
						Name:             current.Name,
						Checksum:         current.Checksum,
						Request:          current.Request,
						RequestTimestamp: current.RequestTimestamp,
						Timestamp:        current.Timestamp,
						Status:           current.Status,
						Details:          current.Details,
					}
					writeTx := w.inmem.Txn(true)
					writeTx.Delete("job", current)
					writeTx.Commit()
					w.queue <- &copy
				}
				tx.Abort()
			} else {
				fmt.Print("sleep")
				w.scheduler.Sleep()
			}
		}
	}
}

func (w *worker) run() {
	for {
		select {
		case <-w.exit:
			return
		default:
			if len(w.queue) > 0 {
				go func(data chan *schema.Job) {
					current := <-w.queue
					if current != nil {
						ctx, cancel_func := context.WithCancel(context.Background())
						w.cancel_funcs[current.Id] = cancel_func
						w.commands[current.Name](ctx, current.Id, current.Request)
					}
				}(w.queue)
			}
		}
	}

}
