package scheduler

import (
	"context"
	"sync"
	"time"
)

type Task func(ctx context.Context)

type Scheduler struct {
	waitGroup     *sync.WaitGroup
	cancellations []context.CancelFunc // cancel functions
}

func NewScheduler() *Scheduler {
	return &Scheduler{
		waitGroup:     new(sync.WaitGroup),
		cancellations: make([]context.CancelFunc, 0),
	}
}

// Register new task
func (s *Scheduler) Register(parentContext context.Context, task Task, interval time.Duration) {
	// Fork child context
	childContext, cancelFunc := context.WithCancel(parentContext)

	// Add cancel function
	s.cancellations = append(s.cancellations, cancelFunc)

	s.waitGroup.Add(1)
	s.process(childContext, task, interval)
}

// Stop all running tasks
func (s *Scheduler) Stop() {
	for _, cancel := range s.cancellations {
		cancel()
	}
	// Wait till all task finished
	s.waitGroup.Wait()
}

func (s *Scheduler) process(childContext context.Context, task Task, interval time.Duration) {
	ticker := time.NewTicker(interval)

	for {
		select {
		case <-ticker.C:
			task(childContext)
		case <-childContext.Done():
			s.waitGroup.Done()
			return
		}
	}
}
