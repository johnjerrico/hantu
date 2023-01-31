package hantu

import (
	"errors"
	"time"

	"github.com/johnjerrico/hantu/scheduler"
	"github.com/johnjerrico/hantu/schema"
	"github.com/johnjerrico/hantu/worker"

	"github.com/hashicorp/go-memdb"
)

type Option struct {
	Id        string
	Domain    string
	RedisHost string
	RedisPort string
	RedisTTL  uint64
	Interval  time.Duration
	TTL       time.Duration
	Max       int
}

type Server interface {
	Worker() worker.Worker
	Dequeue(job schema.Job) error
	Queue(job schema.Job) error
}

type server struct {
	opt   Option
	inmem *memdb.MemDB
	w     worker.Worker
}

var instance Server

func New(opt Option) Server {
	db, err := memdb.NewMemDB(schema.Schema())
	if err != nil {
		panic(err)
	}
	sch := scheduler.New(opt.RedisHost, opt.RedisPort, opt.RedisTTL)
	return &server{
		w: worker.New(
			opt.Domain,
			opt.Id,
			opt.Max,
			opt.Interval,
			db,
			sch,
		),
		inmem: db,
		opt:   opt,
	}
}
func Singleton(opt Option) Server {
	if instance == nil {
		instance = New(opt)
	}
	return instance
}

func (w *server) Worker() worker.Worker {
	return w.w
}

func (w *server) Dequeue(job schema.Job) error {
	tx := w.inmem.Txn(true)
	err := tx.Delete("job", &job)
	tx.Commit()
	return err
}

func (w *server) Queue(job schema.Job) error {
	rtx := w.inmem.Snapshot().Txn(false)
	it, err := rtx.Get("job", "id", job.Id)
	if err != nil {
		return err
	}
	if it.Next() == nil {
		tx := w.inmem.Txn(true)
		copy := schema.Job{
			Id:               job.Id,
			Name:             job.Name,
			Checksum:         job.Checksum,
			Request:          job.Request,
			RequestTimestamp: job.RequestTimestamp,
			Timestamp:        job.Timestamp,
			Status:           job.Status,
		}
		err := tx.Insert("job", &copy)
		tx.Commit()
		return err
	} else {
		return errors.New("already_exists")
	}

}
