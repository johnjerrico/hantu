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

const (
	statusQueued     = "queued"
	statusProcessing = "processing"
)

type Command func(ctx context.Context, request any)

type Worker interface {
	Start()
	Stop()
	Register(name string, cmd Command)
}

func New(domain, id string, max int, inmem *memdb.MemDB) Worker {
	if max == 0 {
		max = 10
	}
	return &worker{
		max:       max,
		commands:  make(map[string]Command),
		exit:      make(chan struct{}),
		inmem:     inmem,
		startOnce: sync.Once{},
		stopOnce:  sync.Once{},
	}
}

type worker struct {
	max       int
	commands  map[string]Command
	exit      chan struct{}
	inmem     *memdb.MemDB
	mu        sync.RWMutex
	startOnce sync.Once
	stopOnce  sync.Once
}

func (w *worker) Register(name string, cmd Command) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.commands[name] == nil {
		w.commands[name] = cmd
	}
}

func (w *worker) Start() {
	w.startOnce.Do(func() {
		go w.spawn()
	})
}

func (w *worker) Stop() {
	w.stopOnce.Do(func() {
		close(w.exit)
	})
}

func (w *worker) spawn() {
	for {
		select {
		case <-w.exit:
			return
		default:
		}

		jobs := w.claimQueuedJobs()
		if len(jobs) == 0 {
			time.Sleep(time.Millisecond)
			continue
		}

		c := limiter.NewConcurrencyLimiter(w.max)
		for _, job := range jobs {
			job := job
			c.Execute(func() {
				w.runJob(job)
			})
		}
		c.WaitAndClose()
	}
}

func (w *worker) claimQueuedJobs() []schema.Job {
	tx := w.inmem.Txn(true)
	defer tx.Abort()

	it, err := tx.Get("job", "status", statusQueued)
	if err != nil {
		return nil
	}

	var jobs []schema.Job
	for obj := it.Next(); obj != nil; obj = it.Next() {
		current := obj.(*schema.Job)
		job := *current

		if err := tx.Delete("job", current); err != nil {
			continue
		}
		job.Status = statusProcessing
		if err := tx.Insert("job", &job); err != nil {
			continue
		}
		jobs = append(jobs, job)
	}

	tx.Commit()
	return jobs
}

func (w *worker) runJob(job schema.Job) {
	w.mu.RLock()
	cmd := w.commands[job.Name]
	w.mu.RUnlock()
	if cmd == nil {
		w.requeue(job)
		return
	}

	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("worker panic recovered: job=%s name=%s panic=%v\n", job.Id, job.Name, r)
			w.requeue(job)
		}
	}()

	if job.Delay > 0 {
		time.Sleep(job.Delay)
	}

	execCtx := job.Ctx
	if execCtx == nil {
		execCtx = context.Background()
	}

	cmd(execCtx, job.Request)
	w.deleteJob(job.Id)
}

func (w *worker) requeue(job schema.Job) {
	job.Status = statusQueued
	tx := w.inmem.Txn(true)
	defer tx.Abort()

	it, err := tx.Get("job", "id", job.Id)
	if err != nil {
		return
	}
	current := it.Next()
	if current != nil {
		if err := tx.Delete("job", current); err != nil {
			return
		}
	}
	if err := tx.Insert("job", &job); err != nil {
		return
	}
	tx.Commit()
}

func (w *worker) deleteJob(id string) {
	tx := w.inmem.Txn(true)
	defer tx.Abort()

	it, err := tx.Get("job", "id", id)
	if err != nil {
		return
	}
	current := it.Next()
	if current == nil {
		return
	}
	if err := tx.Delete("job", current); err != nil {
		return
	}
	tx.Commit()
}
