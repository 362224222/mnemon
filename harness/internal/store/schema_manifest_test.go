package store

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestSchemaManifestCacheBuildsOnceAndReturnsCopies(t *testing.T) {
	t.Parallel()
	started, release := make(chan struct{}), make(chan struct{})
	var builds atomic.Int32
	cache := &schemaManifestCache{build: func(context.Context) ([]namedSchemaObject, error) {
		if builds.Add(1) == 1 {
			close(started)
		}
		<-release
		return testSchemaManifest(), nil
	}}
	const callers = 16
	results := make(chan []namedSchemaObject, callers)
	errorsSeen := make(chan error, callers)
	var group sync.WaitGroup
	for index := 0; index < callers; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			objects, err := cache.load(context.Background())
			results <- objects
			errorsSeen <- err
		}()
	}
	<-started
	close(release)
	group.Wait()
	close(results)
	close(errorsSeen)
	for err := range errorsSeen {
		if err != nil {
			t.Fatal(err)
		}
	}
	var first []namedSchemaObject
	for objects := range results {
		if len(objects) != 1 || objects[0].name != "node" {
			t.Fatalf("cached schema manifest = %#v", objects)
		}
		if first == nil {
			first = objects
		}
	}
	if builds.Load() != 1 {
		t.Fatalf("schema manifest builds = %d", builds.Load())
	}
	first[0].name = "mutated"
	objects, err := cache.load(context.Background())
	if err != nil || objects[0].name != "node" || builds.Load() != 1 {
		t.Fatalf("immutable cached schema manifest = (%#v,%v), builds %d", objects, err, builds.Load())
	}
}

func TestSchemaManifestCacheCancellationDoesNotPoisonRetry(t *testing.T) {
	t.Parallel()
	started := make(chan struct{})
	var builds atomic.Int32
	cache := &schemaManifestCache{build: func(ctx context.Context) ([]namedSchemaObject, error) {
		if builds.Add(1) == 1 {
			close(started)
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return testSchemaManifest(), nil
	}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := cache.load(ctx)
		done <- err
	}()
	<-started
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled schema manifest build error = %v", err)
	}
	objects, err := cache.load(context.Background())
	if err != nil || len(objects) != 1 || builds.Load() != 2 {
		t.Fatalf("schema manifest retry = (%#v,%v), builds %d", objects, err, builds.Load())
	}
}

func TestSchemaManifestCacheWaiterCanCancel(t *testing.T) {
	t.Parallel()
	started, release := make(chan struct{}), make(chan struct{})
	cache := &schemaManifestCache{build: func(context.Context) ([]namedSchemaObject, error) {
		close(started)
		<-release
		return testSchemaManifest(), nil
	}}
	done := make(chan error, 1)
	go func() {
		_, err := cache.load(context.Background())
		done <- err
	}()
	<-started
	waitCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := cache.load(waitCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled schema manifest waiter error = %v", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func testSchemaManifest() []namedSchemaObject {
	return []namedSchemaObject{{name: "node", object: schemaObject{objectType: "table"}}}
}
