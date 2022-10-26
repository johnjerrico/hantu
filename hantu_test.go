package hantu

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/johnjerrico/hantu/schema"
)

func TestWorkerStart(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(6)
	bgworker := New(Option{
		Domain: "tests",
		Id:     "1",
		Max:    7,
		TTL:    5 * time.Second,
	})
	bgworker.Worker().RegisterCommand("test",
		func(ctx context.Context, id string, request interface{}) error {
			for i := 0; i < 10000000000; i++ {
				//do nothing
			}
			t.Log("Processing : " + request.(string))
			wg.Done()
			return nil
		},
	)
	bgworker.Worker().Start()
	bgworker.Queue(schema.Job{
		Id:      "1",
		Name:    "test",
		Request: "1",
	})
	bgworker.Queue(schema.Job{
		Id:      "2",
		Name:    "test",
		Request: "2",
	})
	bgworker.Queue(schema.Job{
		Id:      "3",
		Name:    "test",
		Request: "3",
	})
	bgworker.Queue(schema.Job{
		Id:      "4",
		Name:    "test",
		Request: "4",
	})
	bgworker.Queue(schema.Job{
		Id:      "5",
		Name:    "test",
		Request: "5",
	})
	bgworker.Queue(schema.Job{
		Id:      "6",
		Name:    "test",
		Request: "6",
	})
	wg.Wait()
	bgworker.Worker().Stop()
}

func TestWorkerCancel(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(6)
	bgworker := New(Option{
		Domain:   "tests",
		Id:       "1",
		Max:      7,
		TTL:      1 * time.Second,
		Interval: 3 * time.Second,
	})
	bgworker.Worker().RegisterCommand("test",
		func(ctx context.Context, id string, request interface{}) error {
			time.Sleep(20 * time.Second)

			t.Log("Processing : " + request.(string))
			wg.Done()
			return nil
		},
	)
	bgworker.Worker().RegisterChecksum(
		"checksum",
		func(ctx context.Context, request []schema.Job) ([]string, error) {
			return []string{"2", "3", "4"}, nil
		},
	)

	bgworker.Worker().Start()
	bgworker.Queue(schema.Job{
		Id:       "1",
		Name:     "test",
		Checksum: "checksum",
		Request:  "1",
	})
	bgworker.Queue(schema.Job{
		Id:       "2",
		Name:     "test",
		Checksum: "checksum",
		Request:  "2",
	})
	bgworker.Queue(schema.Job{
		Id:       "3",
		Name:     "test",
		Checksum: "checksum",
		Request:  "3",
	})
	bgworker.Queue(schema.Job{
		Id:       "4",
		Name:     "test",
		Checksum: "checksum",
		Request:  "4",
	})
	bgworker.Queue(schema.Job{
		Id:       "5",
		Name:     "test",
		Checksum: "checksum",
		Request:  "5",
	})
	bgworker.Queue(schema.Job{
		Id:       "6",
		Name:     "test",
		Checksum: "checksum",
		Request:  "6",
	})
	time.Sleep(1 * time.Microsecond)
	bgworker.Worker().Stop()
	wg.Wait()
}
