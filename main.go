package main

import (
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"sync"

	"github.com/gin-gonic/gin"
)

type Message struct {
	Id         string     `json:"id"`
	Queue_name string     `json:"queue_name"`
	Content    string     `json:"content"`
	Next       *Message   `json:"-"`
	mutex      sync.Mutex `json:"-"`
}

func main() {
	r := gin.Default()

	queue_count := flag.Int("count", 0, "Queue count")
	flag.Parse()

	if *queue_count == 0 {
		fmt.Println("Queue count can not be 0!")
		return
	}

	var (
		queues_buffers       = make(map[string][]Message)
		queues_iterators     = make(map[string]int)
		queues_iterators_end = make(map[string]int)
		queue                = make(map[string]Message)
		mutex_iterator       sync.Mutex
		mutex_iterator_end   sync.Mutex
		wg                   sync.WaitGroup
		chan_flag            = make(chan bool)
	)

	wg.Add(*queue_count)
	for range *queue_count {
		go func() {
			defer wg.Done()

			uid, err := GenerateUID()
			if err != nil {
				fmt.Println("Error with generate name for queue")
				return
			}

			mutex_iterator.Lock()

			queue[uid] = Message{
				Id: "",
			}
			queues_buffers[uid] = make([]Message, 10_000)
			queues_iterators[uid] = 0
			queues_iterators_end[uid] = 0

			mutex_iterator.Unlock()

			fmt.Println(uid)

		}()
	}

	wg.Wait()

	r.POST("/in", func(c *gin.Context) {

		var m Message
		c.Bind(&m)

		qb, e := queues_buffers[m.Queue_name]
		if !e {
			c.String(404, "Queue doesn't exist!")
		}

		go func() {

			mutex_iterator_end.Lock()

			iterator, _ := queues_iterators_end[m.Queue_name]
			if iterator == 10_000 {
				for {
					select {
					case <-chan_flag:

						queues_iterators_end[m.Queue_name] = 1
						qb = qb[:0]
						qb[0] = m
						queues_buffers[m.Queue_name] = qb
					}
				}
			}

			mutex_iterator_end.Unlock()
		}()
	})
}

func GenerateUID() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}
