// Copyright (c) 2026 Neomantra Corp

package scratch

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func open(t *testing.T) *Scratch {
	t.Helper()
	s, err := Open(t.TempDir(), "s1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestValidKey(t *testing.T) {
	for k, ok := range map[string]bool{
		"plan": true, "chicago.mft5-nfa8.notes": true, "a_b": true,
		"": false, ".hidden": false, "-x": false, "a..b": false, "UPPER": false,
		"has space": false, "a/b": false, strings.Repeat("a", 65): false,
	} {
		if (ValidKey(k) == nil) != ok {
			t.Errorf("ValidKey(%q) ok=%v, want %v", k, ValidKey(k) == nil, ok)
		}
	}
}

func TestSetGetAppendDeleteListInBothScopes(t *testing.T) {
	s := open(t)
	for _, scope := range []Scope{Session, Global} {
		if err := s.Set(scope, "plan", "one"); err != nil {
			t.Fatal(err)
		}
		if err := s.Append(scope, "plan", " two"); err != nil {
			t.Fatal(err)
		}
		if v, err := s.Get(scope, "plan", 0, 0); err != nil || v != "one two" {
			t.Errorf("%s get = %q, %v", scope, v, err)
		}
	}
	// Scopes are separate.
	if err := s.Set(Global, "only-global", "g"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(Session, "only-global", 0, 0); err == nil {
		t.Error("a global note must not be visible in session scope")
	}
	// Empty scope means session.
	if v, _ := s.Get("", "plan", 0, 0); v != "one two" {
		t.Errorf("default scope read %q", v)
	}
	es, _ := s.List(Global)
	if len(es) != 2 || es[0].Key != "only-global" || es[1].Key != "plan" {
		t.Errorf("list = %+v", es)
	}
	if err := s.Delete(Global, "plan"); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(Global, "plan"); err == nil || !strings.Contains(err.Error(), "unknown key") {
		t.Errorf("second delete: %v", err)
	}
}

func TestGetHeadAndTail(t *testing.T) {
	s := open(t)
	if err := s.Set(Session, "n", "0123456789"); err != nil {
		t.Fatal(err)
	}
	if v, _ := s.Get(Session, "n", 3, 0); v != "012" {
		t.Errorf("head = %q", v)
	}
	if v, _ := s.Get(Session, "n", 0, 3); v != "789" {
		t.Errorf("tail = %q", v)
	}
	if v, _ := s.Get(Session, "n", 99, 0); v != "0123456789" {
		t.Errorf("oversized head = %q", v)
	}
}

func TestLimitsSayWhatToDo(t *testing.T) {
	s := open(t)
	err := s.Set(Session, "big", strings.Repeat("x", 16<<10+1))
	if err == nil || !strings.Contains(err.Error(), "Summarise") {
		t.Errorf("value limit: %v", err)
	}
	for i := 0; i < 32; i++ {
		if err := s.Set(Session, "k"+strings.Repeat("a", i), "v"); err != nil {
			t.Fatalf("key %d: %v", i, err)
		}
	}
	err = s.Set(Session, "one-too-many", "v")
	if err == nil || !strings.Contains(err.Error(), "Delete or merge") {
		t.Errorf("key limit: %v", err)
	}
	// Replacing an existing key at the limit is allowed.
	if err := s.Set(Session, "k", "replaced"); err != nil {
		t.Errorf("replace at key limit: %v", err)
	}
	// Global has more room.
	for i := 0; i < 33; i++ {
		if err := s.Set(Global, "g"+strings.Repeat("a", i), "v"); err != nil {
			t.Fatalf("global key %d: %v", i, err)
		}
	}
}

func TestAppendCountsTheCombinedSize(t *testing.T) {
	s := open(t)
	if err := s.Set(Session, "n", strings.Repeat("x", 16<<10)); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(Session, "n", "y"); err == nil {
		t.Error("append past the value limit should fail")
	}
	if v, _ := s.Get(Session, "n", 0, 0); len(v) != 16<<10 {
		t.Errorf("a failed append must not change the note, len=%d", len(v))
	}
}

func TestHostOwnedKeyIsReadableButNotWritableByTheModel(t *testing.T) {
	s := open(t)
	if err := s.HostSet(KeyLatestRequest, "how many cases"); err != nil {
		t.Fatal(err)
	}
	if v, err := s.Get(Session, KeyLatestRequest, 0, 0); err != nil || v != "how many cases" {
		t.Errorf("read: %q %v", v, err)
	}
	for name, err := range map[string]error{
		"set":    s.Set(Session, KeyLatestRequest, "x"),
		"append": s.Append("", KeyLatestRequest, "x"),
		"delete": s.Delete(Session, KeyLatestRequest),
	} {
		if err == nil || !strings.Contains(err.Error(), "written by the host") {
			t.Errorf("%s: %v", name, err)
		}
	}
	// The reserved name is only reserved in session scope.
	if err := s.Set(Global, KeyLatestRequest, "fine"); err != nil {
		t.Errorf("global %s: %v", KeyLatestRequest, err)
	}
}

func TestSymlinkedNoteIsRefused(t *testing.T) {
	s := open(t)
	outside := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(outside, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(s.Dir, "sessions", "s1")
	if err := os.Symlink(outside, filepath.Join(dir, "evil")); err != nil {
		t.Skip("no symlinks:", err)
	}
	if v, err := s.Get(Session, "evil", 0, 0); err == nil {
		t.Errorf("read through a symlink returned %q", v)
	}
	es, _ := s.List(Session)
	for _, e := range es {
		if e.Key == "evil" {
			t.Error("a symlink must not be listed as a note")
		}
	}
}

func TestResumeKeepsNotesAndGlobalIsShared(t *testing.T) {
	dir := t.TempDir()
	a, _ := Open(dir, "alpha")
	if err := a.Set(Session, "plan", "p"); err != nil {
		t.Fatal(err)
	}
	if err := a.Set(Global, "lesson", "l"); err != nil {
		t.Fatal(err)
	}
	a.Close()

	again, _ := Open(dir, "alpha")
	defer again.Close()
	if v, _ := again.Get(Session, "plan", 0, 0); v != "p" {
		t.Errorf("resumed session lost its note: %q", v)
	}
	other, _ := Open(dir, "beta")
	defer other.Close()
	if _, err := other.Get(Session, "plan", 0, 0); err == nil {
		t.Error("a different session must not see alpha's notes")
	}
	if v, _ := other.Get(Global, "lesson", 0, 0); v != "l" {
		t.Errorf("global note not shared: %q", v)
	}
}

func TestOpenRejectsBadSessionAndScope(t *testing.T) {
	if _, err := Open(t.TempDir(), "../escape"); err == nil {
		t.Error("a session name must not escape the directory")
	}
	s := open(t)
	if _, err := s.List("galaxy"); err == nil || !strings.Contains(err.Error(), "unknown scope") {
		t.Errorf("bad scope: %v", err)
	}
}

func TestBriefListsKeysNotContents(t *testing.T) {
	s := open(t)
	if s.Brief() != "" {
		t.Errorf("empty pads have no brief: %q", s.Brief())
	}
	if err := s.HostSet(KeyLatestRequest, "hello"); err != nil {
		t.Fatal(err)
	}
	if err := s.Set(Session, "plan", "SECRET-CONTENT"); err != nil {
		t.Fatal(err)
	}
	if err := s.Set(Global, "chicago.crimes", "x"); err != nil {
		t.Fatal(err)
	}
	b := s.Brief()
	if !strings.Contains(b, "global notes: chicago.crimes (1 B)") || !strings.Contains(b, "session notes: plan") {
		t.Errorf("brief = %q", b)
	}
	if strings.Contains(b, "SECRET-CONTENT") || strings.Contains(b, KeyLatestRequest) {
		t.Errorf("brief leaks contents or lists the host key: %q", b)
	}
}
