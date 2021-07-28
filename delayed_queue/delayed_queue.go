package delayed_queue

import (
	"errors"
	guuid "github.com/google/uuid"
	"sync"
	"time"
)

var (
	worker = delayedWorker{
		queue:       map[string]delayedJob{},
		mutexQueue:  sync.RWMutex{},
		mutexTicker: sync.RWMutex{},
		stop:        make(chan bool),
	}

	ErrIncorrectDuration = errors.New("incorrect duration")
	ErrJobNotExist       = errors.New("was not added or has already been completed")
)

type delayedWorker struct {
	queue       map[string]delayedJob
	mutexQueue  sync.RWMutex
	ticker      *time.Ticker
	mutexTicker sync.RWMutex
	stop        chan bool
}

func (w *delayedWorker) add(ID string, job delayedJob) {
	w.mutexQueue.Lock()
	w.queue[ID] = job
	w.mutexQueue.Unlock()

	w.startProcessIfNeeded()
}

func (w *delayedWorker) remove(jobID string) (bool, error) {
	w.mutexQueue.Lock()
	defer w.mutexQueue.Unlock()

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
	w.mutexQueue.RLock()
	IDsForRemoved := make([]string, 0)
	for jobID, job := range w.queue {
		now := time.Now()
		if job.RunAt.Equal(now) || job.RunAt.Before(now) {
			IDsForRemoved = append(IDsForRemoved, jobID)
			go job.Callback()
		}
	}
	w.mutexQueue.RUnlock()

	if len(IDsForRemoved) == 0 {
		return
	}

	for _, jobID := range IDsForRemoved {
		w.remove(jobID)
	}
}

func (w *delayedWorker) startProcessIfNeeded() {
	w.mutexTicker.RLock()
	if w.ticker != nil {
		w.mutexTicker.RUnlock()
		return
	}
	w.mutexTicker.RUnlock()

	w.mutexTicker.Lock()
	w.ticker = time.NewTicker(100 * time.Millisecond)
	w.mutexTicker.Unlock()

	go func() {
		for {
			select {
			case <-w.stop:
				w.mutexTicker.Lock()
				w.ticker.Stop()
				w.ticker = nil
				w.mutexTicker.Unlock()

				return
			case <-w.ticker.C:
				go w.loop()
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
