package main

import (
	"delayed_queue/delayed_queue"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"
)

func main() {
	wg := sync.WaitGroup{}
	sec := 12
	parallels := 10000
	problem := 0
	for i := sec; i > 4; i-- {
		for j := 1; j <= parallels; j++ {
			go func(i, j int) {
				wg.Add(1)
				start := time.Now()
				delayed_queue.AddJob(time.Duration(i)*time.Second, func() {
					dur := time.Now().Sub(start)
					if int(dur.Seconds()) != i {
						problem++
						//fmt.Printf("%d (%d) - %+v run\n", i, j, dur)
					}

					wg.Done()
				})

				// fmt.Printf("job #%s scheduled %ds\n", jobID, i)
			}(i, j)
			runtime.Gosched()
		}
	}

	wg.Wait()
	all := (sec - 5) * parallels
	fmt.Printf("complette (%d * %d = %d, problems: %d (%d))\n", sec-5, parallels, all, problem, problem*100/all)
	os.Exit(0)
}
