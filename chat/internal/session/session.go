// Copyright (c) 2026 Neomantra Corp

// Package session records one run as JSON lines, one event per line, so a
// run can be replayed, graded, or queried with DuckDB's read_json_auto.
//
// Rows never go into the file: a presentation is recorded by its shape.
package session

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/neomantra/CivicSodaQuack/chat/internal/agent"
	"github.com/neomantra/CivicSodaQuack/chat/internal/present"
)

// Record is one line of the file. Type says which optional fields are set.
type Record struct {
	Time    time.Time `json:"time"`
	Session string    `json:"session"`
	Seq     int       `json:"seq"`
	Type    string    `json:"type"` // session_start | turn | tool_call | present

	// session_start
	Model   string   `json:"model,omitempty"`
	Portals []string `json:"portals,omitempty"`

	// turn, tool_call, present
	Turn int `json:"turn,omitempty"`

	// turn
	Prompt        string `json:"prompt,omitempty"`
	Response      string `json:"response,omitempty"`
	PromptChars   int    `json:"prompt_chars,omitempty"`
	ResponseChars int    `json:"response_chars,omitempty"`
	ToolCalls     int    `json:"tool_calls,omitempty"`
	Presented     int    `json:"presented,omitempty"`
	Steps         int    `json:"steps,omitempty"`
	Usage         *Usage `json:"usage,omitempty"`

	// tool_call
	Tool        string `json:"tool,omitempty"`
	Input       string `json:"input,omitempty"`
	Rows        int    `json:"rows,omitempty"`
	OutputBytes int    `json:"output_bytes,omitempty"`

	// present
	Presentation *present.Summary `json:"presentation,omitempty"`

	// turn, tool_call
	DurationMS int64  `json:"duration_ms,omitempty"`
	Error      string `json:"error,omitempty"`
}

// Usage is the token accounting for a turn.
type Usage struct {
	Input         int64 `json:"input"`
	Output        int64 `json:"output"`
	Reasoning     int64 `json:"reasoning,omitempty"`
	CacheRead     int64 `json:"cache_read,omitempty"`
	CacheCreation int64 `json:"cache_creation,omitempty"`
}

// Recorder appends records to one file. A nil Recorder is safe to use and
// records nothing, so callers need no branches when sessions are disabled.
type Recorder struct {
	mu   sync.Mutex
	f    *os.File
	id   string
	seq  int
	turn int
	path string
}

// Open creates <dir>/<timestamp>-<id>.jsonl. An empty dir disables
// recording and returns nil.
func Open(dir string) (*Recorder, error) {
	if dir == "" {
		return nil, nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("session dir: %w", err)
	}
	now := time.Now().UTC()
	id := fmt.Sprintf("%s-%d", now.Format("20060102T150405"), os.Getpid())
	path := filepath.Join(dir, id+".jsonl")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o644)
	if err != nil {
		return nil, fmt.Errorf("session file: %w", err)
	}
	return &Recorder{f: f, id: id, path: path}, nil
}

// Path is where the file lives, or "" for a nil Recorder.
func (r *Recorder) Path() string {
	if r == nil {
		return ""
	}
	return r.path
}

// Close flushes and closes the file.
func (r *Recorder) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.f.Close()
}

func (r *Recorder) write(rec Record) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seq++
	rec.Seq = r.seq
	rec.Session = r.id
	if rec.Time.IsZero() {
		rec.Time = time.Now().UTC()
	}
	b, err := json.Marshal(rec)
	if err != nil {
		return
	}
	_, _ = r.f.Write(append(b, '\n'))
}

// Start records the model and portals for the run.
func (r *Recorder) Start(model string, portals []string) {
	r.write(Record{Type: "session_start", Model: model, Portals: portals})
}

// BeginTurn advances the turn counter that tool and present records carry.
func (r *Recorder) BeginTurn() int {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.turn++
	return r.turn
}

// Event records a tool call or presentation. Progress lines are not kept;
// they are for the screen.
func (r *Recorder) Event(ev agent.Event) {
	if r == nil {
		return
	}
	r.mu.Lock()
	turn := r.turn
	r.mu.Unlock()
	switch ev.Kind {
	case agent.EventToolCall:
		r.write(Record{
			Type: "tool_call", Turn: turn, Tool: ev.Tool, Input: ev.Input,
			Rows: ev.Rows, OutputBytes: ev.OutputBytes,
			DurationMS: ev.Duration.Milliseconds(), Error: ev.Err,
		})
	case agent.EventPresent:
		if ev.Presentation == nil {
			return
		}
		s := ev.Presentation.Summary()
		r.write(Record{Type: "present", Turn: turn, Presentation: &s})
	}
}

// Turn records a finished turn. resp may be the zero value when err is set.
func (r *Recorder) Turn(prompt string, resp agent.Response, err error) {
	if r == nil {
		return
	}
	r.mu.Lock()
	turn := r.turn
	r.mu.Unlock()
	rec := Record{
		Type: "turn", Turn: turn,
		Prompt: prompt, PromptChars: len([]rune(prompt)),
		Response: resp.Text, ResponseChars: len([]rune(resp.Text)),
		ToolCalls: resp.ToolCalls, Presented: resp.Presented, Steps: resp.Steps,
		DurationMS: resp.Duration.Milliseconds(),
	}
	if err != nil {
		rec.Error = err.Error()
	} else {
		rec.Usage = &Usage{
			Input: resp.Usage.InputTokens, Output: resp.Usage.OutputTokens,
			Reasoning: resp.Usage.ReasoningTokens,
			CacheRead: resp.Usage.CacheReadTokens, CacheCreation: resp.Usage.CacheCreationTokens,
		}
	}
	r.write(rec)
}
