package hantu

import (
	"errors"
	"sync"

	"github.com/johnjerrico/hantu/schema"
	"github.com/johnjerrico/hantu/worker"

	"github.com/hashicorp/go-memdb"
)

type Option struct {
	Id     string
	Domain string
	Max    int
}

type Server interface {
	Worker() worker.Worker
	Dequeue(job schema.Job) error
	Queue(job schema.Job) error
	Count() (int, error)
}

type server struct {
	opt   Option
	inmem *memdb.MemDB
	w     worker.Worker
}

var instance Server
var singletonOnce sync.Once

func New(opt Option) Server {
	db, err := memdb.NewMemDB(schema.Schema())
	if err != nil {
		panic(err)
	}
	//sch := scheduler.New(opt.RedisHost, opt.RedisPort, opt.RedisTTL)
	return &server{
		w: worker.New(
			opt.Domain,
			opt.Id,
			opt.Max,
			db,
			//sch,
		),
		inmem: db,
		opt:   opt,
	}
}
func Singleton(opt Option) Server {
	singletonOnce.Do(func() {
		instance = New(opt)
		instance.Worker().Start()
	})
	return instance
}

func (w *server) Worker() worker.Worker {
	return w.w
}

func (w *server) Dequeue(job schema.Job) error {
	tx := w.inmem.Txn(true)
	defer tx.Abort()
	it, err := tx.Get("job", "id", job.Id)
	if err != nil {
		return err
	}
	current := it.Next()
	if current == nil {
		return errors.New("not_found")
	}
	if err := tx.Delete("job", current); err != nil {
		return err
	}
	tx.Commit()
	return nil
}

func (w *server) Queue(job schema.Job) error {
	tx := w.inmem.Txn(true)
	defer tx.Abort()
	status := job.Status
	if status == "" {
		status = "queued"
	}
	copy := schema.Job{
		Id:               job.Id,
		Name:             job.Name,
		Request:          job.Request,
		RequestTimestamp: job.RequestTimestamp,
		Timestamp:        job.Timestamp,
		Status:           status,
		Delay:            job.Delay,
		Ctx:              job.Ctx,
	}
	if err := tx.Insert("job", &copy); err != nil {
		return errors.New("already_exists")
	}
	tx.Commit()
	return nil
}

func (w *server) Count() (int, error) {
	rtx := w.inmem.Snapshot().Txn(false)
	it, err := rtx.Get("job", "id")
	if err != nil {
		return -1, err
	}
	total := 0
	for obj := it.Next(); obj != nil; obj = it.Next() {
		total++
	}
	rtx.Abort()
	return total, nil
}
