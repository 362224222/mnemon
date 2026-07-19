package integration

import (
	"context"
	"sync"
)

type hostActivationWatcher struct {
	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once
}

func startHostActivationWatcher(ctx context.Context, processGroupID int) *hostActivationWatcher {
	watcher := &hostActivationWatcher{
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	go watcher.run(ctx, processGroupID)
	return watcher
}

func (watcher *hostActivationWatcher) run(ctx context.Context, processGroupID int) {
	defer close(watcher.done)
	select {
	case <-ctx.Done():
		terminateHostActivationProcessGroup(processGroupID)
	case <-watcher.stop:
	}
}

func (watcher *hostActivationWatcher) stopAndWait() {
	if watcher == nil {
		return
	}
	watcher.stopOnce.Do(func() { close(watcher.stop) })
	<-watcher.done
}
