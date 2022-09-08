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
type Checksum func(ctx context.Context, jobs []schema.Job) error
type c struct {
	ctx      context.Context
	cancel   context.CancelFunc
	cmd      Command
	checksum Checksum
}

type Worker interface {
	Start()
	Stop()
	Register(name string, cmd Command, checksum Checksum)
}

func New(domain, id string, max int, interval time.Duration, inmem *memdb.MemDB, scheduler scheduler.Scheduler) Worker {
	return &worker{
		max:       max,
		interval:  interval,
		commands:  make(map[string]c),
		queue:     make(chan *schema.Job, max),
		exit:      make(chan byte),
		inmem:     inmem,
		scheduler: scheduler,
	}
}

type worker struct {
	id        string
	domain    string
	max       int
	interval  time.Duration
	commands  map[string]c
	queue     chan *schema.Job
	exit      chan byte
	inmem     *memdb.MemDB
	scheduler scheduler.Scheduler
}

func (w *worker) Register(name string, cmd Command, checksum Checksum) {
	ctx, cancel := context.WithCancel(context.Background())
	w.commands[name] = c{
		ctx:      ctx,
		cmd:      cmd,
		cancel:   cancel,
		checksum: checksum,
	}
}

func (w *worker) Start() {
	w.scheduler.Register(w.domain, w.id)
	w.checksum(w.interval)
	go w.spawn()
	go w.run()
}

func (w *worker) Stop() {
	w.exit <- byte('1')
	close(w.queue)
	for _, item := range w.commands {
		item.cancel()
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
					processor := make(map[string][]schema.Job)
					for obj := it.Next(); obj != nil; obj = it.Next() {
						current := obj.(*schema.Job)
						if len(processor[current.Cheksum]) == 0 {
							processor[current.Cheksum] = make([]schema.Job, 0)
						}
						processor[current.Cheksum] = append(processor[current.Cheksum], *current)
					}
					tx.Abort()
					for checksum_func, pending := range processor {
						w.commands[checksum_func].checksum(
							w.commands[checksum_func].ctx,
							pending,
						)
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
						Cheksum:          current.Cheksum,
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
					current := <-data
					if current != nil {
						w.commands[current.Name].cmd(w.commands[current.Name].ctx, current.Id, current.Request)
					}
				}(w.queue)
			}
		}
	}

}
