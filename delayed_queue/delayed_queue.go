package delayed_queue

import (
	"errors"
	"fmt"
	guuid "github.com/google/uuid"
	"sync"
	"time"
)

var (
	worker = delayedWorker{
		queue: map[string]delayedJob{},
		mutex: sync.RWMutex{},
		stop:  make(chan bool),
	}

	ErrIncorrectDuration = errors.New("incorrect duration")
	ErrJobNotExist       = errors.New("was not added or has already been completed")
)

type delayedWorker struct {
	queue  map[string]delayedJob
	mutex  sync.RWMutex
	ticker *time.Ticker
	stop   chan bool
}

func (w *delayedWorker) add(ID string, job delayedJob) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	w.queue[ID] = job
	w.startProcessIfNeeded()
}

func (w *delayedWorker) remove(jobID string) (bool, error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if _, exist := w.queue[jobID]; !exist {
		return false, ErrJobNotExist
	}

	delete(w.queue, jobID)

	if len(w.queue) == 0 {
		w.stop <- true
	}

	return true, nil
}

func (w *delayedWorker) loop() {
	w.mutex.RLock()
	IDsForRemoved := make([]string, 0)
	for jobID, job := range w.queue {
		if job.RunAt.Before(time.Now()) {
			IDsForRemoved = append(IDsForRemoved, jobID)
			go job.Callback()
		}
	}
	w.mutex.RUnlock()

	if len(IDsForRemoved) == 0 {
		return
	}

	for _, jobID := range IDsForRemoved {
		w.remove(jobID)
	}
}

func (w *delayedWorker) startProcessIfNeeded() {
	if w.ticker != nil {
		return
	}

	w.ticker = time.NewTicker(1 * time.Second)
	go func() {
		for {
			select {
			case <-w.stop:
				w.ticker.Stop()
				w.ticker = nil
				fmt.Println("stop")

				return
			case <-w.ticker.C:
				w.loop()
			}
		}
	}()
}

type delayedJob struct {
	RunAt    time.Time
	Callback func()
}

func AddJob(after time.Duration, callback func()) (string, error) {
	now := time.Now()
	runAt := now.Add(after)

	if now.After(runAt) {
		return "", ErrIncorrectDuration
	}

	job := delayedJob{
		RunAt:    runAt,
		Callback: callback,
	}

	ID := guuid.NewString()
	worker.add(ID, job)

	return ID, nil
}

func RemoveJob(jobID string) (bool, error) {
	return worker.remove(jobID)
}
