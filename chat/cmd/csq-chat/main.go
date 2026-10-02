// Copyright (c) 2026 Neomantra Corp
//
// csq-chat — a terminal chat over CivicSodaQuack databases.
//
// The model reads the attached portals through the same handlers csq's MCP
// server uses and pushes tables to the screen. With --prompt it runs one
// turn without a terminal and prints the reply and what would have been
// shown, which is how scripts and tests drive it.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	tea "charm.land/bubbletea/v2"
	flag "github.com/spf13/pflag"

	"github.com/charmbracelet/x/ansi"

	"github.com/neomantra/CivicSodaQuack/chat/internal/agent"
	"github.com/neomantra/CivicSodaQuack/chat/internal/chart"
	"github.com/neomantra/CivicSodaQuack/chat/internal/data"
	"github.com/neomantra/CivicSodaQuack/chat/internal/present"
	"github.com/neomantra/CivicSodaQuack/chat/internal/session"
	"github.com/neomantra/CivicSodaQuack/chat/internal/tui"
)

const usage = `csq-chat — chat with CivicSodaQuack data

Usage:
  csq-chat --db <portal.duckdb> [--db alias=path ...] -m <vendor/model> [options]
  csq-chat --db <portal.duckdb> -m <vendor/model> --prompt "question"   # headless, one turn

Models are named vendor/model: anthropic/claude-sonnet-4-6, openai/gpt-5,
google/gemini-2.5-pro, openrouter/<model>, or compat/<model> with --base-url
for any OpenAI-compatible server (a local Kronk server, for example).
API keys come from --api-key or the vendor's environment variable
(ANTHROPIC_API_KEY, OPENAI_API_KEY, GEMINI_API_KEY, OPENROUTER_API_KEY).

Options:
`

type options struct {
	dbs        []string
	model      string
	baseURL    string
	apiKey     string
	prompt     string
	sessionDir string
	logFile    string
	timeout    time.Duration
	maxSteps   int
	maxOutput  int64
	maxHistory int
	verbose    bool
}

// newModel builds the language model; tests swap in a scripted one.
var newModel = agent.NewLanguageModel

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "csq-chat: %v\n", err)
		os.Exit(1)
	}
}

func parseFlags(args []string, stderr io.Writer) (options, error) {
	var o options
	fs := flag.NewFlagSet("csq-chat", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprint(stderr, usage)
		fs.PrintDefaults()
	}
	fs.StringArrayVar(&o.dbs, "db", nil, "csq DuckDB file to attach, as path or alias=path (repeatable)")
	fs.StringVarP(&o.model, "model", "m", envOr("CSQ_CHAT_MODEL", ""), "vendor/model to talk to")
	fs.StringVar(&o.baseURL, "base-url", envOr("CSQ_CHAT_BASE_URL", ""), "base URL for compat/ models")
	fs.StringVar(&o.apiKey, "api-key", envOr("CSQ_CHAT_API_KEY", ""), "API key; overrides the vendor's environment variable")
	fs.StringVarP(&o.prompt, "prompt", "p", "", "run one turn headless and print the result")
	fs.StringVar(&o.sessionDir, "session-dir", envOr("CSQ_CHAT_SESSION_DIR", defaultSessionDir()), "where to write session JSONL files; empty disables")
	fs.StringVar(&o.logFile, "log-file", envOr("CSQ_CHAT_LOG", ""), "append logs here (the TUI owns the terminal); empty means stderr in headless mode, discarded in the TUI")
	fs.DurationVar(&o.timeout, "timeout", 2*time.Minute, "per-turn timeout")
	fs.IntVar(&o.maxSteps, "max-steps", 12, "model calls allowed per turn")
	fs.Int64Var(&o.maxOutput, "max-output-tokens", 8192, "output tokens allowed per model call")
	fs.IntVar(&o.maxHistory, "max-history", 12, "messages kept between turns")
	fs.BoolVarP(&o.verbose, "verbose", "v", false, "headless: print progress lines to stderr")
	if err := fs.Parse(args); err != nil {
		return o, err
	}
	if len(o.dbs) == 0 {
		return o, errors.New("at least one --db is required")
	}
	if o.model == "" {
		return o, errors.New("--model (or CSQ_CHAT_MODEL) is required")
	}
	return o, nil
}

func run(args []string, stdout, stderr io.Writer) error {
	o, err := parseFlags(args, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger, closeLog, err := newLogger(o, stderr)
	if err != nil {
		return err
	}
	defer closeLog()

	store, err := data.Open(o.dbs)
	if err != nil {
		return err
	}
	defer store.Close()

	charts, err := chart.New(ctx)
	if err != nil {
		return err
	}
	defer charts.Close(context.Background())

	opts := agent.Options{
		Charts: charts,
		Model:  o.model, BaseURL: o.baseURL, APIKey: o.apiKey,
		Timeout: o.timeout, MaxSteps: o.maxSteps, MaxOutputTokens: o.maxOutput, MaxHistory: o.maxHistory,
	}
	model, err := newModel(ctx, opts)
	if err != nil {
		return err
	}
	runner := agent.New(model, store, opts)

	rec, err := session.Open(o.sessionDir)
	if err != nil {
		return err
	}
	defer rec.Close()
	rec.Start(runner.Model(), store.Portals())
	if rec.Path() != "" {
		logger.Info("session file", "path", rec.Path())
	}

	chat := &recordedChat{runner: runner, rec: rec, logger: logger}
	if o.prompt != "" {
		return runHeadless(ctx, chat, charts, o, stdout, stderr)
	}
	return runTUI(ctx, chat, charts, tui.Info{Portals: store.Portals(), SessionPath: rec.Path()})
}

// recordedChat is the agent as the window sees it, with every turn written
// to the session file on the way through.
type recordedChat struct {
	runner *agent.Runner
	rec    *session.Recorder
	logger *slog.Logger
}

func (c *recordedChat) Ask(ctx context.Context, prompt string, progress agent.ProgressFunc, show present.Func) (agent.Response, error) {
	c.rec.BeginTurn()
	resp, err := c.runner.Ask(ctx, prompt, func(ev agent.Event) {
		c.rec.Event(ev)
		if ev.Kind == agent.EventProgress {
			c.logger.Debug(ev.Message)
		}
		if progress != nil {
			progress(ev)
		}
	}, show)
	c.rec.Turn(prompt, resp, err)
	if err != nil {
		c.logger.Error("turn failed", "err", err)
	}
	return resp, err
}

func (c *recordedChat) ClearHistory() { c.runner.ClearHistory() }
func (c *recordedChat) Model() string { return c.runner.Model() }

func runTUI(ctx context.Context, chat tui.Chat, charts tui.ChartView, info tui.Info) error {
	m := tui.New(ctx, chat, info).WithCharts(charts)
	_, err := tea.NewProgram(m, tea.WithContext(ctx)).Run()
	if errors.Is(err, tea.ErrProgramKilled) || errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

// headlessOutput is what --prompt prints: the reply and what would have
// been shown, as one JSON document.
type headlessOutput struct {
	Prompt        string                 `json:"prompt"`
	Response      string                 `json:"response"`
	ToolCalls     int                    `json:"tool_calls"`
	Steps         int                    `json:"steps"`
	DurationMS    int64                  `json:"duration_ms"`
	Presentations []present.Presentation `json:"presentations"`
	// Rendered holds each chart as plain text, in presentation order, so a
	// run can be read without a terminal. Tables are not repeated here.
	Rendered []Rendered `json:"rendered,omitempty"`
}

// Rendered is one chart drawn at 80×20 with styling removed.
type Rendered struct {
	Title    string   `json:"title"`
	Text     string   `json:"text"`
	Warnings []string `json:"warnings,omitempty"`
	Error    string   `json:"error,omitempty"`
}

func runHeadless(ctx context.Context, chat tui.Chat, charts tui.ChartView, o options, stdout, stderr io.Writer) error {
	var shown []present.Presentation
	var progress agent.ProgressFunc
	if o.verbose {
		progress = func(ev agent.Event) {
			if ev.Kind == agent.EventProgress {
				fmt.Fprintln(stderr, "· "+ev.Message)
			}
		}
	}
	resp, err := chat.Ask(ctx, o.prompt, progress, func(p present.Presentation) { shown = append(shown, p) })
	if err != nil {
		return err
	}
	out := headlessOutput{
		Prompt: o.prompt, Response: resp.Text, ToolCalls: resp.ToolCalls, Steps: resp.Steps,
		DurationMS: resp.Duration.Milliseconds(), Presentations: shown,
	}
	if out.Presentations == nil {
		out.Presentations = []present.Presentation{}
	}
	for _, p := range shown {
		if p.Chart == nil {
			continue
		}
		r := Rendered{Title: p.Title}
		view, warnings, err := charts.Render(*p.Chart, 80, 20)
		if err != nil {
			r.Error = err.Error()
		} else {
			r.Text, r.Warnings = ansi.Strip(view), warnings
		}
		out.Rendered = append(out.Rendered, r)
	}
	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}

func newLogger(o options, stderr io.Writer) (*slog.Logger, func(), error) {
	level := slog.LevelInfo
	if o.verbose {
		level = slog.LevelDebug
	}
	var w io.Writer
	closeFn := func() {}
	switch {
	case o.logFile != "":
		f, err := os.OpenFile(o.logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return nil, nil, fmt.Errorf("log file: %w", err)
		}
		w = f
		closeFn = func() { f.Close() }
	case o.prompt != "":
		w = stderr
	default:
		// The TUI owns the terminal; unrouted logs would corrupt it.
		w = io.Discard
	}
	return slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: level})), closeFn, nil
}

func defaultSessionDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".csq", "sessions")
}

func envOr(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}
