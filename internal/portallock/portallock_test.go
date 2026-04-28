// Copyright (c) 2026 Neomantra Corp

package portallock

import (
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestLock_AcquireRelease(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "x.duckdb")
	l, err := Acquire(path, Options{})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if err := l.Release(); err != nil {
		t.Errorf("release: %v", err)
	}
	l2, err := Acquire(path, Options{})
	if err != nil {
		t.Errorf("re-acquire: %v", err)
	}
	_ = l2.Release()
}

func TestLock_Contention_NoWait(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "x.duckdb")
	l, err := Acquire(path, Options{})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer l.Release()

	_, err = Acquire(path, Options{LockWait: 0})
	if err == nil {
		t.Fatal("want contention error")
	}
	if !strings.Contains(err.Error(), "locked") {
		t.Errorf("error should mention locked: %v", err)
	}
}

func TestLock_Contention_WaitSucceeds(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "x.duckdb")
	l, _ := Acquire(path, Options{})

	go func() {
		time.Sleep(100 * time.Millisecond)
		_ = l.Release()
	}()

	start := time.Now()
	l2, err := Acquire(path, Options{LockWait: time.Second})
	if err != nil {
		t.Fatalf("acquire-with-wait: %v", err)
	}
	defer l2.Release()
	if time.Since(start) > 500*time.Millisecond {
		t.Errorf("acquire took too long: %v", time.Since(start))
	}
}

func TestLock_Contention_WaitTimeout(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "x.duckdb")
	l, _ := Acquire(path, Options{})
	defer l.Release()

	start := time.Now()
	_, err := Acquire(path, Options{LockWait: 100 * time.Millisecond})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("want timeout error")
	}
	if elapsed < 100*time.Millisecond {
		t.Errorf("returned too fast (%v)", elapsed)
	}
	if elapsed > 500*time.Millisecond {
		t.Errorf("timeout overshoot (%v)", elapsed)
	}
}

func TestLock_NoLock_Bypass(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "x.duckdb")
	l, err := Acquire(path, Options{NoLock: true})
	if err != nil {
		t.Fatalf("noop acquire: %v", err)
	}
	if err := l.Release(); err != nil {
		t.Errorf("noop release: %v", err)
	}
	l2, err := Acquire(path, Options{})
	if err != nil {
		t.Errorf("real acquire after noop: %v", err)
	}
	_ = l2.Release()
}

func TestLock_PathNotWritable(t *testing.T) {
	_, err := Acquire("/nonexistent-dir-xyz/x.duckdb", Options{})
	if err == nil {
		t.Fatal("want error for non-writable path")
	}
}

func TestLock_ConcurrentSameProcess(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "x.duckdb")
	var wg sync.WaitGroup
	successes := 0
	var mu sync.Mutex
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l, err := Acquire(path, Options{LockWait: 2 * time.Second})
			if err != nil {
				t.Errorf("concurrent acquire: %v", err)
				return
			}
			mu.Lock()
			successes++
			mu.Unlock()
			time.Sleep(50 * time.Millisecond)
			_ = l.Release()
		}()
	}
	wg.Wait()
	if successes != 2 {
		t.Errorf("both goroutines should have eventually acquired; got %d", successes)
	}
}
