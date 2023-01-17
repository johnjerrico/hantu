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
		func(ctx context.Context, request interface{}) {
			for i := 0; i < 10000000000; i++ {
				//do nothing
			}
			t.Log("Processing : " + request.(string))
			wg.Done()
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
	//var wg sync.WaitGroup
	//wg.Add(6)
	bgworker := New(Option{
		Domain:   "tests",
		Id:       "1",
		Max:      7,
		TTL:      1 * time.Second,
		Interval: 3 * time.Second,
	})
	bgworker.Worker().RegisterCommand("test",
		func(ctx context.Context, request interface{}) {
			select {
			case <-ctx.Done():
				return
			default:
				time.Sleep(20 * time.Second)
				t.Log("Processing : " + request.(string))
			}

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
	bgworker.Worker().Stop()
	//wg.Wait()
	time.Sleep(29 * time.Second)
}

func TestWorkerChecksum(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(2)
	bgworker := New(Option{
		Domain:   "tests",
		Id:       "1",
		Max:      3,
		TTL:      1 * time.Second,
		Interval: 1 * time.Second,
	})
	bgworker.Worker().RegisterCommand("test",
		func(ctx context.Context, request interface{}) {
			select {
			case <-ctx.Done():
				wg.Done()
			default:
				t.Log("Processing : " + " -> " + request.(string))
				time.Sleep(10 * time.Second)
				wg.Done()
			}

		},
	)
	bgworker.Worker().RegisterChecksum(
		"checksum",
		func(ctx context.Context, checksum string, request []schema.Job) ([]schema.Job, []schema.Job, error) {
			for _, val := range request {
				if val.Id == "1" || val.Id == "2" {
					return nil, nil, nil
				}
			}
			return []schema.Job{
				{
					Id:       "1",
					Name:     "test",
					Checksum: "checksum",
					Request:  "1",
				},
				{
					Id:       "2",
					Name:     "test",
					Checksum: "checksum",
					Request:  "2",
				},
			}, nil, nil
		},
	)

	bgworker.Worker().Start()
	wg.Wait()
	bgworker.Worker().Stop()

}
