// Copyright (c) 2026 Neomantra Corp

// Package portallock provides advisory file-based locking for the per-portal
// DuckDB files. Every CLI subcommand that opens a portal DB acquires
// <dbpath>.lock before opening, releases on exit. NoLock skips the lock; LockWait
// retries with exponential backoff before failing.
package portallock

import (
	"fmt"
	"time"

	"github.com/gofrs/flock"
)

// Options controls lock acquisition behavior.
type Options struct {
	NoLock   bool          // skip locking entirely; Acquire returns a no-op Lock
	LockWait time.Duration // max retry duration; 0 = fail-fast
}

// Lock represents an acquired portal lock. Caller must Release.
// A Lock returned with NoLock=true is a no-op sentinel.
type Lock struct {
	flock *flock.Flock // nil when NoLock
	path  string       // for error messages
}

// Acquire takes an exclusive lock on <dbpath>.lock. When opts.NoLock is true,
// returns a sentinel Lock whose Release is a no-op.
func Acquire(dbpath string, opts Options) (*Lock, error) {
	if opts.NoLock {
		return &Lock{path: dbpath}, nil
	}

	lockPath := dbpath + ".lock"
	fl := flock.New(lockPath)

	got, err := fl.TryLock()
	if err != nil {
		return nil, fmt.Errorf("cannot create lock file %s: %w", lockPath, err)
	}
	if got {
		return &Lock{flock: fl, path: dbpath}, nil
	}

	if opts.LockWait <= 0 {
		return nil, fmt.Errorf("portal database is locked by another process: %s (pass --no-lock to bypass, or --lock-wait <duration> to retry)", lockPath)
	}

	deadline := time.Now().Add(opts.LockWait)
	wait := 50 * time.Millisecond
	for time.Now().Before(deadline) {
		time.Sleep(wait)
		got, err := fl.TryLock()
		if err != nil {
			return nil, fmt.Errorf("cannot acquire lock %s: %w", lockPath, err)
		}
		if got {
			return &Lock{flock: fl, path: dbpath}, nil
		}
		wait = wait * 3 / 2
		if wait > time.Second {
			wait = time.Second
		}
	}
	return nil, fmt.Errorf("portal database is locked: %s; waited %v", lockPath, opts.LockWait)
}

// Release releases the lock and closes the underlying file descriptor.
// Safe to call multiple times; safe on a NoLock sentinel.
func (l *Lock) Release() error {
	if l == nil || l.flock == nil {
		return nil
	}
	err := l.flock.Unlock()
	l.flock = nil
	return err
}
