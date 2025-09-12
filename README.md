# Hantu
Provides a simple configurable background worker library which works by using fire and forget principle. The library is also built with microservice in mind; thus every time the server restart, any idle job at that server will be transfered to another running instance.
## Functions
1. Start
	```
        bgworker.Worker().Start()
	```
2. Stop
	```
        bgworker.Worker().Stop()
	```
3. Register
	```
		bgworker.Worker().Register(
		"test",
		context.Background(),
	    func (ctx context.Context, request any) (any,error) {
			time.Sleep(5 * time.Second)
			fmt.Println("Processing: " + request.(string))
			wg.Done()
			return nil,nil
	    })	
	```
4. Queue
	```
		bgworker.Queue(schema.Job{
			Id: "1",
			Name: "test",
			Request: "1",
		})
	```
5. Dequeue
	```
		bgworker.Dequeue(schema.Job{
			Id: "1",
			Name: "test",
			Request: "1",
		})
	```
## Schema
```
type  Job  struct {
	Id string
	Name string
	Cheksum string
	Request string
	RequestTimestamp string
	Timestamp string
	Status string
	Details string
}
```
## Options

```
Id string // unique process identifier

Domain string // worker name

RedisHost string // redis (support live beyond the service)

RedisPort string // redis (support live beyond the service)

RedisTTL uint64 // redis (support live beyond the service)

Interval time.Duration // checksum interval

TTL time.Duration // length of time to live 

Max int // max number of conccurencies
```
## Basic Example

 ```
package main

import (
"context"
"time"
"sync"
"github.com/johnjerrico/hantu/schema"
"github.com/johnjerrico/hantu"
)
func main(){
   var wg sync.WaitGroup
   wg.Add(6)
   bgworker := hantu.New(hantu.Option{
	Domain: "test",
	Id: "1",
	Max: 7, // max conccurencies
	TTL: 5 * time.Second, // time to live
   })
   //register worker
   bgworker.Worker().Register(
	"test",
	context.Background(),
	func (ctx context.Context, request any) (any,error) {
		time.Sleep(5 * time.Second)
		fmt.Println("Processing: " + request.(string))
		wg.Done()
		return nil,nil
   })
   bgworker.Worker().Start()
   bgworker.Queue(schema.Job{
		Id: "1",
		Name: "test",
		Request: "1",
   })
   bgworker.Queue(schema.Job{
		Id: "2",
		Name: "test",
		Request: "2",
   })
   bgworker.Queue(schema.Job{
		Id: "3",
		Name: "test",
		Request: "3",
   })
   bgworker.Queue(schema.Job{
		Id: "4",
		Name: "test",
		Request: "4",
   })
   bgworker.Queue(schema.Job{
		Id: "5",
		Name: "test",
		Request: "5",
   })
   bgworker.Queue(schema.Job{
		Id: "6",
		Name: "test",
		Request: "6",
   })
   wg.Wait()
   bgworker.Worker().Stop()
}
```


