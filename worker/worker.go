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
type Checksum func(ctx context.Context, name string, jobs []schema.Job) (shouldRun []schema.Job, shouldCancel []schema.Job, err error)

type Worker interface {
	Start()
	Stop()
	RegisterCommand(name string, cmd Command)
	RegisterChecksum(name string, checksum Checksum)
}

func New(domain, id string, max int, interval time.Duration, inmem *memdb.MemDB /*, scheduler scheduler.Scheduler*/) Worker {
	return &worker{
		max:          max,
		interval:     interval,
		commands:     make(map[string]Command),
		checksums:    make(map[string]Checksum),
		exit:         make(chan byte),
		cancel_funcs: make(map[string]context.CancelFunc),
		inmem:        inmem,
		//scheduler:    scheduler,
	}
}

type worker struct {
	//id           string
	//domain       string
	max          int
	interval     time.Duration
	commands     map[string]Command
	checksums    map[string]Checksum
	exit         chan byte
	cancel_funcs map[string]context.CancelFunc
	inmem        *memdb.MemDB
	//scheduler    scheduler.Scheduler
	mutex sync.RWMutex
}

func (w *worker) RegisterCommand(name string, cmd Command) {
	w.commands[name] = cmd
}

func (w *worker) RegisterChecksum(name string, checksum Checksum) {
	w.checksums[name] = checksum
}

func (w *worker) Start() {
	/* disabling distributed feature
	//w.scheduler.Register(w.domain, w.id)
	//w.checksum(w.interval)*/
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
	//w.scheduler.Shutdown()
	w.exit <- byte('1')

}

/*
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
					total := 0
					for obj := it.Next(); obj != nil; obj = it.Next() {
						total++
						current := obj.(*schema.Job)
						if len(tobeProcessed[current.Checksum]) == 0 {
							tobeProcessed[current.Checksum] = make([]schema.Job, 0)
						}
						index[current.Id] = *current
						tobeProcessed[current.Checksum] = append(tobeProcessed[current.Checksum], *current)
					}
					tx.Abort()
					if total+1 < w.max {
						for name, checksum_func := range w.checksums {
							shouldRun, shouldCancel, err := checksum_func(
								context.Background(),
								name,
								tobeProcessed[name],
							)
							if err != nil {
								fmt.Println(err.Error())
							} else {

								if len(shouldCancel) > 0 {
									for _, job := range shouldCancel {
										current := index[job.Id]
										if current.Id != "" {
											writeTx := w.inmem.Txn(true)
											writeTx.Delete("job", current)
											writeTx.Commit()
											if w.cancel_funcs[job.Id] != nil {
												w.cancel_funcs[job.Id]()
												w.cancel_funcs[job.Id] = nil
											}
										}
									}
								}
								if len(shouldRun) > 0 {
									for _, job := range shouldRun {
										rtx := w.inmem.Snapshot().Txn(false)
										it, _ := rtx.Get("job", "id", job.Id)
										defer rtx.Abort()
										if it.Next() == nil {
											copy := schema.Job{
												Id:               job.Id,
												Name:             job.Name,
												Checksum:         job.Checksum,
												Request:          job.Request,
												RequestTimestamp: job.RequestTimestamp,
												Timestamp:        job.Timestamp,
												Status:           job.Status,
											}
											writeTx := w.inmem.Txn(true)
											if err := writeTx.Insert("job", &copy); err != nil {
												fmt.Println(err)
											}
											writeTx.Commit()
										}

									}
								}

							}
						}
					}
					w.scheduler.Sleep()
				}
			}
			time.Sleep(time.Millisecond)
		}
	}()
} */

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
