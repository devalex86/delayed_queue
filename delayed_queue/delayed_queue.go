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
		queue: []*delayedJob{},
	}

	ErrIncorrectDuration = errors.New("incorrect duration")
	ErrJobNotExist       = errors.New("was not added or has already been completed")
)

type delayedWorker struct {
	queue      []*delayedJob
	mutexQueue sync.RWMutex
	timer      *time.Timer
	nextRunAt  time.Time

	lastClean      time.Time
	lastCleanMutex sync.RWMutex
}

func (w *delayedWorker) add(job *delayedJob) {
	w.mutexQueue.Lock()
	defer w.mutexQueue.Unlock()

	w.queue = append(w.queue, job)
	if w.nextRunAt.IsZero() || w.nextRunAt.After(job.RunAt) {
		go w.loop()
	}
}

func (w *delayedWorker) loop() {
	now := time.Now()
	if w.timer != nil {
		w.timer.Stop()
	}

	nextRunAt := time.Time{}
	w.mutexQueue.RLock()
	needQueueClean := false
	for _, job := range w.queue {
		if job.IsComplete() {
			continue
		}

		if job.RunAt.Equal(now) || job.RunAt.Before(now) {
			go job.Run()
			needQueueClean = true
		} else if nextRunAt.IsZero() || job.RunAt.Before(nextRunAt) {
			nextRunAt = job.RunAt
		}
	}
	w.mutexQueue.RUnlock()

	if !nextRunAt.IsZero() {
		w.timer = time.AfterFunc(nextRunAt.Sub(time.Now()), w.loop)
		if w.nextRunAt.IsZero() || nextRunAt.Before(w.nextRunAt) {
			w.mutexQueue.Lock()
			w.nextRunAt = nextRunAt
			w.mutexQueue.Unlock()
		}
	}

	if needQueueClean {
		go w.cleanIfNeeded()
	}
}

func (w *delayedWorker) cleanIfNeeded() {
	w.lastCleanMutex.RLock()
	dur := time.Now().Sub(w.lastClean).Seconds()
	if !w.lastClean.IsZero() && dur <= 1 {
		w.lastCleanMutex.RUnlock()
		return
	}

	w.lastCleanMutex.RUnlock()
	w.lastCleanMutex.Lock()
	w.lastClean = time.Now()
	w.lastCleanMutex.Unlock()

	w.mutexQueue.Lock()
	defer w.mutexQueue.Unlock()

	removed := 0
	queue := make([]*delayedJob, 0, len(w.queue))
	for _, job := range w.queue {
		if !job.IsComplete() {
			queue = append(queue, job)
		} else {
			removed++
		}
	}
	w.queue = queue

	fmt.Printf("removed %d, left %d (%.0f)\n", removed, len(w.queue), dur)
}

func (w *delayedWorker) cancel(jobID string) (bool, error) {
	w.mutexQueue.Lock()
	defer w.mutexQueue.Unlock()

	for _, job := range w.queue {
		if job.ID == jobID && !job.IsComplete() {
			job.Cancel()
			return true, nil
		}
	}

	return false, ErrJobNotExist
}

type delayedJob struct {
	ID       string
	RunAt    time.Time
	callback func()

	complete bool
	mutex    sync.RWMutex
}

func (j *delayedJob) Run() {
	j.mutex.Lock()
	defer j.mutex.Unlock()

	if j.complete {
		return
	}

	j.complete = true
	j.callback()
}

func (j *delayedJob) IsComplete() bool {
	j.mutex.RLock()
	defer j.mutex.RUnlock()

	return j.complete
}

func (j *delayedJob) Cancel() {
	j.mutex.RLock()
	if j.complete {
		j.mutex.RUnlock()
		return
	}
	j.mutex.RUnlock()

	j.mutex.Lock()
	defer j.mutex.Unlock()

	j.complete = true
}

func AddJob(after time.Duration, callback func()) (string, error) {
	now := time.Now()
	runAt := now.Add(after)

	if now.After(runAt) {
		return "", ErrIncorrectDuration
	}

	ID := guuid.NewString()
	go worker.add(&delayedJob{
		ID:       ID,
		RunAt:    runAt,
		callback: callback,
	})

	return ID, nil
}

func CancelJob(jobID string) (bool, error) {
	return worker.cancel(jobID)
}
