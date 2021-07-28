package main

import (
	"delayed_queue/delayed_queue"
	"fmt"
	"os"
	"sync"
	"time"
)

func main() {
	now := time.Now()

	wg := sync.WaitGroup{}
	for i := 60; i > 0; i-- {
		for j := 1; j <= 3; j++ {
			go func(i, j int) {
				wg.Add(1)

				delayed_queue.AddJob(time.Duration(i)*time.Second, func() {
					dur := time.Now().Sub(now)
					if int(dur.Seconds()) != i {
						fmt.Printf("%d (%d) - %+v run\n", i, j, dur)
					}

					wg.Done()
				})

				// fmt.Printf("job #%s scheduled %ds\n", jobID, i)
			}(i, j)
		}
	}

	wg.Wait()
	os.Exit(0)
}
