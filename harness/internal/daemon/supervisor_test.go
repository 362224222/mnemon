package daemon

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSupervisorRunsWorkersAndRecordsReports(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	ctx := context.Background()
	supervisor := NewSupervisor([]Worker{
		workerFunc{
			name: "multica-watch",
			kind: WorkerInteraction,
			run: func(ctx context.Context, reporter Reporter) error {
				reporter.Report(WorkerReport{Name: "multica-watch", Kind: WorkerInteraction, Status: "idle", Message: "cursor=7"})
				return nil
			},
		},
		workerFunc{
			name: "managed-drive",
			kind: WorkerDrive,
			run: func(context.Context, Reporter) error {
				return nil
			},
		},
	}, func() time.Time { return now })

	if err := supervisor.Run(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot := supervisor.Snapshot()
	if len(snapshot.Workers) != 2 {
		t.Fatalf("snapshot workers = %+v", snapshot.Workers)
	}
	if snapshot.Workers["managed-drive"].Status != "completed" {
		t.Fatalf("drive worker status = %+v", snapshot.Workers["managed-drive"])
	}
	if snapshot.Workers["multica-watch"].Kind != WorkerInteraction {
		t.Fatalf("interaction worker kind = %+v", snapshot.Workers["multica-watch"])
	}
}

func TestSupervisorCapturesWorkerFailure(t *testing.T) {
	supervisor := NewSupervisor([]Worker{
		workerFunc{
			name: "project-multica",
			kind: WorkerProjection,
			run: func(context.Context, Reporter) error {
				return errors.New("projection failed")
			},
		},
	}, func() time.Time { return time.Unix(100, 0).UTC() })

	err := supervisor.Run(context.Background())
	if err == nil {
		t.Fatal("expected worker error")
	}
	snapshot := supervisor.Snapshot()
	got := snapshot.Workers["project-multica"]
	if got.Status != "failed" || got.Error != "projection failed" {
		t.Fatalf("failure snapshot = %+v", got)
	}
}

func TestSupervisorRequiresWorkers(t *testing.T) {
	if err := NewSupervisor(nil, nil).Run(context.Background()); err == nil {
		t.Fatal("expected no-worker error")
	}
}

type workerFunc struct {
	name string
	kind WorkerKind
	run  func(context.Context, Reporter) error
}

func (w workerFunc) Name() string {
	return w.name
}

func (w workerFunc) Kind() WorkerKind {
	return w.kind
}

func (w workerFunc) Run(ctx context.Context, reporter Reporter) error {
	return w.run(ctx, reporter)
}
