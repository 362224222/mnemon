package daemon

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type WorkerKind string

const (
	WorkerInteraction WorkerKind = "interaction"
	WorkerDrive       WorkerKind = "drive"
	WorkerSurface     WorkerKind = "surface"
	WorkerStatus      WorkerKind = "status"
)

type Worker interface {
	Name() string
	Kind() WorkerKind
	Run(context.Context, Reporter) error
}

type Reporter interface {
	Report(WorkerReport)
}

type WorkerReport struct {
	Name    string
	Kind    WorkerKind
	Status  string
	Message string
	At      time.Time
}

type Snapshot struct {
	StartedAt time.Time                 `json:"started_at"`
	Workers   map[string]WorkerSnapshot `json:"workers"`
}

type WorkerSnapshot struct {
	Kind      WorkerKind `json:"kind"`
	Status    string     `json:"status"`
	Message   string     `json:"message,omitempty"`
	StartedAt time.Time  `json:"started_at,omitempty"`
	UpdatedAt time.Time  `json:"updated_at,omitempty"`
	Error     string     `json:"error,omitempty"`
}

type Clock func() time.Time

type Supervisor struct {
	workers []Worker
	now     Clock

	mu       sync.Mutex
	started  bool
	snapshot Snapshot
}

func NewSupervisor(workers []Worker, now Clock) *Supervisor {
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &Supervisor{workers: append([]Worker(nil), workers...), now: now}
}

func (s *Supervisor) Run(ctx context.Context) error {
	if len(s.workers) == 0 {
		return fmt.Errorf("daemon supervisor requires at least one worker")
	}
	startedAt := s.now()
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return fmt.Errorf("daemon supervisor already started")
	}
	s.started = true
	s.snapshot = Snapshot{StartedAt: startedAt, Workers: map[string]WorkerSnapshot{}}
	for _, worker := range s.workers {
		name := worker.Name()
		s.snapshot.Workers[name] = WorkerSnapshot{
			Kind:      worker.Kind(),
			Status:    "starting",
			StartedAt: startedAt,
			UpdatedAt: startedAt,
		}
	}
	s.mu.Unlock()

	var wg sync.WaitGroup
	errs := make(chan error, len(s.workers))
	for _, worker := range s.workers {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			name := worker.Name()
			s.setWorker(name, WorkerSnapshot{
				Kind:      worker.Kind(),
				Status:    "running",
				StartedAt: startedAt,
				UpdatedAt: s.now(),
			})
			if err := worker.Run(ctx, s); err != nil {
				s.setWorker(name, WorkerSnapshot{
					Kind:      worker.Kind(),
					Status:    "failed",
					StartedAt: startedAt,
					UpdatedAt: s.now(),
					Error:     err.Error(),
				})
				errs <- fmt.Errorf("%s worker %q: %w", worker.Kind(), name, err)
				return
			}
			status := "stopped"
			if ctx.Err() == nil {
				status = "completed"
			}
			s.setWorker(name, WorkerSnapshot{
				Kind:      worker.Kind(),
				Status:    status,
				StartedAt: startedAt,
				UpdatedAt: s.now(),
			})
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		return err
	}
	return nil
}

func (s *Supervisor) Report(report WorkerReport) {
	if report.At.IsZero() {
		report.At = s.now()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.snapshot.Workers == nil {
		s.snapshot.Workers = map[string]WorkerSnapshot{}
	}
	current := s.snapshot.Workers[report.Name]
	if current.StartedAt.IsZero() {
		current.StartedAt = report.At
	}
	current.Kind = report.Kind
	current.Status = report.Status
	current.Message = report.Message
	current.UpdatedAt = report.At
	s.snapshot.Workers[report.Name] = current
}

func (s *Supervisor) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := Snapshot{StartedAt: s.snapshot.StartedAt, Workers: map[string]WorkerSnapshot{}}
	for name, worker := range s.snapshot.Workers {
		out.Workers[name] = worker
	}
	return out
}

func (s *Supervisor) setWorker(name string, snapshot WorkerSnapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.snapshot.Workers == nil {
		s.snapshot.Workers = map[string]WorkerSnapshot{}
	}
	if current := s.snapshot.Workers[name]; !current.StartedAt.IsZero() && snapshot.StartedAt.IsZero() {
		snapshot.StartedAt = current.StartedAt
	}
	s.snapshot.Workers[name] = snapshot
}
