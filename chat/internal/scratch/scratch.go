// Copyright (c) 2026 Neomantra Corp
//
// The store is adapted from ds4go's scratchtool package (MIT,
// Copyright (c) 2026 Neomantra Corp): flat keys, one file per key, atomic
// writes, confinement to the pad directory, and size limits. This version
// adds two scopes and host-owned keys, and drops the ds4 tool registry.

// Package scratch is the model's working memory: small text notes that
// outlive a turn, and with the global scope, a run.
//
// Two scopes share one interface. Session notes belong to one named session
// and are the model's plan, findings, and open questions; resuming the
// session name resumes them. Global notes are shared by every session and
// are where the model records what it has learned about the data.
//
// Notes are fallible reference data written by a model. Nothing here is
// instruction, and nothing here feeds csq's own arithmetic.
package scratch

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Scope says whose notes a call touches.
type Scope string

const (
	// Session notes belong to one session name.
	Session Scope = "session"
	// Global notes are shared by every session.
	Global Scope = "global"
)

// Reserved keys are written by the host. The model can read them but not
// change them.
const (
	// KeyLatestRequest holds the person's most recent message, verbatim, so
	// the model can recover it after the conversation history is trimmed.
	KeyLatestRequest = "latest-request"
)

// Limits bound one pad.
type Limits struct {
	MaxKeys       int
	MaxValueBytes int
	MaxTotalBytes int
}

// DefaultLimits are per scope: session notes are small working memory,
// global notes are allowed to grow into reference material.
func DefaultLimits(s Scope) Limits {
	if s == Global {
		return Limits{MaxKeys: 128, MaxValueBytes: 16 << 10, MaxTotalBytes: 512 << 10}
	}
	return Limits{MaxKeys: 32, MaxValueBytes: 16 << 10, MaxTotalBytes: 128 << 10}
}

// Entry describes one key in a listing.
type Entry struct {
	Key      string    `json:"key"`
	Size     int64     `json:"size"`
	Modified time.Time `json:"modified"`
}

const maxKeyLen = 64

// ValidKey checks a key or session name: 1–64 characters from [a-z0-9._-],
// no leading '.' or '-', and no "..". Keys double as file names, so these
// rules also keep every key one safe path component.
func ValidKey(key string) error {
	switch {
	case key == "":
		return errors.New("empty key")
	case len(key) > maxKeyLen:
		return fmt.Errorf("key %q is %d characters; the limit is %d", key, len(key), maxKeyLen)
	case key[0] == '.' || key[0] == '-':
		return fmt.Errorf("key %q must not start with %q", key, string(key[0]))
	case strings.Contains(key, ".."):
		return fmt.Errorf("key %q must not contain %q", key, "..")
	}
	for i := 0; i < len(key); i++ {
		c := key[i]
		if c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == '-' {
			continue
		}
		return fmt.Errorf("key %q contains %q; keys use only a-z, 0-9, '.', '_' and '-'", key, string(key[i]))
	}
	return nil
}

// Pad is one directory of notes.
type Pad struct {
	dir    string
	root   *os.Root
	limits Limits
	mu     sync.Mutex
}

func openPad(dir string, limits Limits) (*Pad, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(abs, 0o700); err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(abs)
	if err != nil {
		return nil, err
	}
	return &Pad{dir: abs, root: root, limits: limits}, nil
}

func (p *Pad) close() error { return p.root.Close() }

func (p *Pad) list() ([]Entry, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.listLocked()
}

func (p *Pad) listLocked() ([]Entry, error) {
	dirents, err := os.ReadDir(p.dir)
	if err != nil {
		return nil, err
	}
	out := make([]Entry, 0, len(dirents))
	for _, d := range dirents {
		if ValidKey(d.Name()) != nil || !d.Type().IsRegular() {
			continue
		}
		info, err := d.Info()
		if err != nil {
			continue
		}
		out = append(out, Entry{Key: d.Name(), Size: info.Size(), Modified: info.ModTime().UTC()})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

// readLocked refuses anything but a regular file, through the root handle so
// a planted symlink cannot lead outside the pad even under a concurrent swap.
func (p *Pad) readLocked(key string) ([]byte, error) {
	info, err := p.root.Lstat(key)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("key %q is not a regular note", key)
	}
	return p.root.ReadFile(key)
}

func (p *Pad) get(key string) ([]byte, error) {
	if err := ValidKey(key); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	v, err := p.readLocked(key)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("unknown key %q", key)
	}
	return v, err
}

func (p *Pad) set(key string, value []byte) error {
	if err := ValidKey(key); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.setLocked(key, value)
}

func (p *Pad) appendTo(key string, value []byte) error {
	if err := ValidKey(key); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	old, err := p.readLocked(key)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return p.setLocked(key, append(old, value...))
}

// setLocked enforces the limits, then writes atomically: a temp file renamed
// over the destination, so a failure never leaves a truncated note.
func (p *Pad) setLocked(key string, value []byte) error {
	if len(value) > p.limits.MaxValueBytes {
		return fmt.Errorf("note %q would be %d bytes; the limit is %d. Summarise, or split it across keys", key, len(value), p.limits.MaxValueBytes)
	}
	entries, err := p.listLocked()
	if err != nil {
		return err
	}
	exists := false
	var total int64
	for _, e := range entries {
		if e.Key == key {
			exists = true
			continue
		}
		total += e.Size
	}
	if !exists && len(entries) >= p.limits.MaxKeys {
		return fmt.Errorf("the pad already holds %d notes; the limit is %d. Delete or merge one first", len(entries), p.limits.MaxKeys)
	}
	if total+int64(len(value)) > int64(p.limits.MaxTotalBytes) {
		return fmt.Errorf("the pad would hold %d bytes; the limit is %d. Delete or shorten a note first", total+int64(len(value)), p.limits.MaxTotalBytes)
	}
	tmp, err := os.CreateTemp(p.dir, "."+key+".tmp.*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	for _, step := range []func() error{
		func() error { _, err := tmp.Write(value); return err },
		tmp.Sync,
		tmp.Close,
	} {
		if err := step(); err != nil {
			tmp.Close()
			os.Remove(name)
			return err
		}
	}
	if err := p.root.Rename(filepath.Base(name), key); err != nil {
		os.Remove(name)
		return err
	}
	return nil
}

func (p *Pad) remove(key string) error {
	if err := ValidKey(key); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	err := p.root.Remove(key)
	if errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("unknown key %q", key)
	}
	return err
}

// Scratch holds both scopes for one session.
type Scratch struct {
	Dir     string
	Session string
	pads    map[Scope]*Pad
}

// DefaultDir is ~/.csq/scratch.
func DefaultDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".csq", "scratch")
}

// Open returns the notes for session under dir. Session notes live in
// <dir>/sessions/<session>, global notes in <dir>/global. Reuse a session
// name to resume its notes.
func Open(dir, session string) (*Scratch, error) {
	if dir == "" {
		return nil, errors.New("scratch: no directory")
	}
	if err := ValidKey(session); err != nil {
		return nil, fmt.Errorf("scratch: bad session name: %w", err)
	}
	s := &Scratch{Dir: dir, Session: session, pads: map[Scope]*Pad{}}
	for scope, path := range map[Scope]string{
		Session: filepath.Join(dir, "sessions", session),
		Global:  filepath.Join(dir, "global"),
	} {
		p, err := openPad(path, DefaultLimits(scope))
		if err != nil {
			s.Close()
			return nil, fmt.Errorf("scratch: %s notes: %w", scope, err)
		}
		s.pads[scope] = p
	}
	return s, nil
}

// Close releases both pads.
func (s *Scratch) Close() error {
	if s == nil {
		return nil
	}
	var first error
	for _, p := range s.pads {
		if err := p.close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (s *Scratch) pad(scope Scope) (*Pad, error) {
	if scope == "" {
		scope = Session
	}
	p, ok := s.pads[scope]
	if !ok {
		return nil, fmt.Errorf("unknown scope %q; use %q or %q", scope, Session, Global)
	}
	return p, nil
}

func reserved(scope Scope, key string) bool {
	return (scope == "" || scope == Session) && key == KeyLatestRequest
}

func errReserved(key string) error {
	return fmt.Errorf("key %q is written by the host; you can read it but not change it", key)
}

// List returns the keys in scope, sorted.
func (s *Scratch) List(scope Scope) ([]Entry, error) {
	p, err := s.pad(scope)
	if err != nil {
		return nil, err
	}
	return p.list()
}

// Get returns a note. head or tail, when positive, return only that many
// bytes from the start or end.
func (s *Scratch) Get(scope Scope, key string, head, tail int) (string, error) {
	p, err := s.pad(scope)
	if err != nil {
		return "", err
	}
	v, err := p.get(key)
	if err != nil {
		return "", err
	}
	switch {
	case head > 0 && head < len(v):
		v = v[:head]
	case tail > 0 && tail < len(v):
		v = v[len(v)-tail:]
	}
	return string(v), nil
}

// Set replaces a note, creating it if missing.
func (s *Scratch) Set(scope Scope, key, value string) error {
	if reserved(scope, key) {
		return errReserved(key)
	}
	p, err := s.pad(scope)
	if err != nil {
		return err
	}
	return p.set(key, []byte(value))
}

// Append adds to a note, creating it if missing.
func (s *Scratch) Append(scope Scope, key, value string) error {
	if reserved(scope, key) {
		return errReserved(key)
	}
	p, err := s.pad(scope)
	if err != nil {
		return err
	}
	return p.appendTo(key, []byte(value))
}

// Delete removes a note.
func (s *Scratch) Delete(scope Scope, key string) error {
	if reserved(scope, key) {
		return errReserved(key)
	}
	p, err := s.pad(scope)
	if err != nil {
		return err
	}
	return p.remove(key)
}

// HostSet writes a reserved key. Only the host calls it.
func (s *Scratch) HostSet(key, value string) error {
	p, err := s.pad(Session)
	if err != nil {
		return err
	}
	return p.set(key, []byte(value))
}

// Brief lists what both scopes hold, for the system prompt of a session that
// starts with notes already present. It reports keys and sizes, never
// contents: the model reads what it needs.
func (s *Scratch) Brief() string {
	var parts []string
	for _, scope := range []Scope{Global, Session} {
		es, err := s.List(scope)
		if err != nil {
			continue
		}
		var keys []string
		for _, e := range es {
			if scope == Session && e.Key == KeyLatestRequest {
				continue
			}
			keys = append(keys, fmt.Sprintf("%s (%d B)", e.Key, e.Size))
		}
		if len(keys) > 0 {
			parts = append(parts, fmt.Sprintf("%s notes: %s", scope, strings.Join(keys, ", ")))
		}
	}
	return strings.Join(parts, "\n")
}
