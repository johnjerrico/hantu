package hantu

import (
	"context"
	"fmt"
	"log"
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
			t.Log("Processing : " + fmt.Sprintf("%v", request))
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

/*
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
				t.Log("Processing : " + fmt.Sprintf("%v", request))
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
*/

func TestPanic(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(6)
	bgworker := New(Option{
		Domain:   "tests",
		Id:       "1",
		Max:      7,
		TTL:      1 * time.Second,
		Interval: 3 * time.Second,
	})
	cnt := 0
	bgworker.Worker().RegisterCommand("test",
		func(ctx context.Context, request interface{}) {
			select {
			case <-ctx.Done():
				return
			default:
				job := request.(*schema.Job)

				if job.Id == "1" {
					panic("test")
				} else {
					//time.Sleep(20 * time.Second)
					fmt.Println("Hello", job.Id)
					//t.Log("Processing : " + fmt.Sprintf("%v", request))

				}

			}
			cnt++
			wg.Done()
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
	wg.Wait()
	bgworker.Worker().Stop()
	log.Print(cnt)

	//time.Sleep(30 * time.Second)
}

/*
	func TestWorkerChecksum(t *testing.T) {
		var wg sync.WaitGroup
		wg.Add(3)
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
					t.Log("Processing : " + " -> " + fmt.Sprintf("%v", request))
					time.Sleep(2 * time.Second)
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
					{
						Id:       "3",
						Name:     "test",
						Checksum: "checksum",
						Request:  "3",
					},
				}, nil, nil
			},
		)

		bgworker.Worker().Start()
		wg.Wait()
		bgworker.Worker().Stop()

}
*/
func TestDoubleQueue(t *testing.T) {
	bgworker := New(Option{
		Domain:   "tests",
		Id:       "1",
		Max:      3,
		TTL:      1 * time.Second,
		Interval: 1 * time.Second,
	})
	bgworker.Queue(schema.Job{
		Id:       "1",
		Name:     "test",
		Checksum: "checksum",
		Request:  "1",
	})
	err := bgworker.Queue(schema.Job{
		Id:       "1",
		Name:     "test",
		Checksum: "checksum",
		Request:  "1",
	})
	if err == nil {
		t.Log("should be error due to duplicate")
		t.Fail()
	}
	fmt.Println(err)
}

func TestCountJobs(t *testing.T) {
	bgworker := New(Option{
		Domain:   "tests",
		Id:       "1",
		Max:      3,
		TTL:      1 * time.Second,
		Interval: 1 * time.Second,
	})
	bgworker.Queue(schema.Job{
		Id:       "1",
		Name:     "test",
		Checksum: "checksum",
		Request:  "1",
	})
	err := bgworker.Queue(schema.Job{
		Id:       "2",
		Name:     "test",
		Checksum: "checksum",
		Request:  "2",
	})
	if err != nil {
		t.Log("should not be error")
		t.Fail()
	}
	total, err := bgworker.Count()
	if err != nil {
		t.Log("should not be error")
		t.Fail()
	}
	if total != 2 {
		t.Log("should be 2 ")
		t.Fail()
	}
	t.Log("total is", total)
}
