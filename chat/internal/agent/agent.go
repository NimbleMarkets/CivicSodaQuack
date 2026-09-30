// Copyright (c) 2026 Neomantra Corp

// Package agent runs the model. A Runner owns a Fantasy agent, the tools
// over csq's data, and a bounded conversation history; Ask runs one turn.
//
// The runner talks to the outside world through two callbacks carried in the
// context: a progress func for what the model is doing, and a present func
// for what a tool wants shown. Neither the TUI nor the headless runner is
// imported here.
package agent

import (
	"context"
	_ "embed"
	"fmt"
	"strings"
	"sync"
	"time"

	"charm.land/fantasy"

	"github.com/neomantra/CivicSodaQuack/chat/internal/data"
	"github.com/neomantra/CivicSodaQuack/chat/internal/present"
)

//go:embed system_prompt.md
var systemPromptTemplate string

// Options tunes a Runner. Zero values take the defaults below.
type Options struct {
	Model   string // "vendor/model"
	BaseURL string
	APIKey  string

	MaxOutputTokens int64         // per turn, default 8192
	MaxSteps        int           // model calls per turn, default 12
	MaxHistory      int           // messages kept between turns, default 12
	MaxMessageChars int           // per kept message, default 6000
	MaxToolBytes    int           // per tool result sent to the model, default 24000
	PresentRowCap   int           // rows pushed to the screen per table, default 500
	Timeout         time.Duration // per turn, default 2m
}

func (o Options) withDefaults() Options {
	if o.MaxOutputTokens <= 0 {
		o.MaxOutputTokens = 8192
	}
	if o.MaxSteps <= 0 {
		o.MaxSteps = 12
	}
	if o.MaxHistory <= 0 {
		o.MaxHistory = 12
	}
	if o.MaxMessageChars <= 0 {
		o.MaxMessageChars = 6000
	}
	if o.MaxToolBytes <= 0 {
		o.MaxToolBytes = 24000
	}
	if o.PresentRowCap <= 0 {
		o.PresentRowCap = 500
	}
	if o.Timeout <= 0 {
		o.Timeout = 2 * time.Minute
	}
	return o
}

// EventKind says what an Event reports.
type EventKind string

const (
	// EventProgress is a human-readable line about what is happening.
	EventProgress EventKind = "progress"
	// EventToolCall records one finished tool call.
	EventToolCall EventKind = "tool_call"
	// EventPresent records that a tool pushed something to the screen.
	EventPresent EventKind = "present"
)

// Event is one thing the runner reports during a turn.
type Event struct {
	Kind    EventKind
	Message string

	// Tool call fields.
	Tool        string
	Input       string
	Rows        int
	OutputBytes int
	Duration    time.Duration
	Err         string

	// Present fields.
	Presentation *present.Presentation
}

// ProgressFunc receives events as they happen.
type ProgressFunc func(Event)

type progressKey struct{}

func emit(ctx context.Context, ev Event) {
	if fn, ok := ctx.Value(progressKey{}).(ProgressFunc); ok && fn != nil {
		fn(ev)
	}
}

// Response is the outcome of one turn.
type Response struct {
	Text      string
	Reasoning string
	ToolCalls int
	Presented int
	Steps     int
	Usage     fantasy.Usage
	Duration  time.Duration
}

// Runner holds one conversation.
type Runner struct {
	agent   fantasy.Agent
	model   fantasy.LanguageModel
	store   data.Store
	opts    Options
	mu      sync.Mutex
	history []fantasy.Message
	turn    int
}

// New builds a Runner over an already constructed model, which is how tests
// supply a fake. Use Open for the model named in opts.
func New(model fantasy.LanguageModel, store data.Store, opts Options) *Runner {
	opts = opts.withDefaults()
	r := &Runner{model: model, store: store, opts: opts}
	r.agent = fantasy.NewAgent(model,
		fantasy.WithSystemPrompt(systemPrompt(store.Portals(), time.Now())),
		fantasy.WithTools(tools(store, opts)...),
		fantasy.WithStopConditions(fantasy.StepCountIs(opts.MaxSteps)),
		fantasy.WithMaxOutputTokens(opts.MaxOutputTokens),
		fantasy.WithMaxRetries(2),
	)
	return r
}

// Open builds the model named by opts.Model and returns a Runner over it.
func Open(ctx context.Context, store data.Store, opts Options) (*Runner, error) {
	model, err := NewLanguageModel(ctx, opts)
	if err != nil {
		return nil, err
	}
	return New(model, store, opts), nil
}

func systemPrompt(portals []string, now time.Time) string {
	list := strings.Join(portals, ", ")
	if list == "" {
		list = "(none)"
	}
	s := strings.ReplaceAll(systemPromptTemplate, "{{portals}}", list)
	return strings.TrimSpace(s) + "\n\nToday is " + now.UTC().Format("2006-01-02") + "."
}

// Model reports the vendor and model in use.
func (r *Runner) Model() string { return r.model.Provider() + "/" + r.model.Model() }

// Ask runs one turn. progress and presentFn may be nil.
func (r *Runner) Ask(ctx context.Context, prompt string, progress ProgressFunc, presentFn present.Func) (Response, error) {
	prompt = strings.TrimSpace(prompt)
	if prompt == "" {
		return Response{}, fmt.Errorf("empty prompt")
	}
	if progress != nil {
		ctx = context.WithValue(ctx, progressKey{}, progress)
	}
	// Count what tools push, but only install a listener when there is a screen:
	// a headless run must let tools see that nothing is showing.
	var presented int
	if presentFn != nil {
		ctx = present.WithFunc(ctx, func(p present.Presentation) {
			presented++
			presentFn(p)
		})
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.opts.Timeout)
		defer cancel()
	}

	r.mu.Lock()
	r.turn++
	history := append([]fantasy.Message(nil), r.history...)
	r.mu.Unlock()

	emit(ctx, Event{Kind: EventProgress, Message: fmt.Sprintf("calling %s with %d history messages", r.Model(), len(history))})
	start := time.Now()
	result, err := r.agent.Generate(ctx, fantasy.AgentCall{
		Prompt:   prompt,
		Messages: history,
	})
	if err != nil {
		emit(ctx, Event{Kind: EventProgress, Message: "model error: " + err.Error()})
		return Response{}, err
	}

	toolCalls := 0
	var reasoning []string
	for _, step := range result.Steps {
		toolCalls += len(step.Content.ToolCalls())
		if rt := strings.TrimSpace(step.Content.ReasoningText()); rt != "" {
			reasoning = append(reasoning, rt)
		}
	}
	stoppedMidWork := len(result.Steps) > 0 &&
		result.Steps[len(result.Steps)-1].FinishReason == fantasy.FinishReasonToolCalls
	text := resolveText(result.Response.Content.Text(), presented, stoppedMidWork)

	r.mu.Lock()
	r.history = append(r.history,
		fantasy.NewUserMessage(truncateText(prompt, r.opts.MaxMessageChars)),
		assistantMessage(truncateText(text, r.opts.MaxMessageChars)),
	)
	r.history = trimHistory(r.history, r.opts.MaxHistory)
	r.mu.Unlock()

	emit(ctx, Event{Kind: EventProgress, Message: fmt.Sprintf("done: %d step(s), %d tool call(s)", len(result.Steps), toolCalls)})
	return Response{
		Text:      text,
		Reasoning: strings.Join(reasoning, "\n\n"),
		ToolCalls: toolCalls,
		Presented: presented,
		Steps:     len(result.Steps),
		Usage:     result.Response.Usage,
		Duration:  time.Since(start),
	}, nil
}

// resolveText gives an empty model reply a truthful line: the table was the
// answer, or the turn ran out of steps before answering.
func resolveText(text string, presented int, stoppedMidWork bool) string {
	text = strings.TrimSpace(text)
	if text != "" {
		return text
	}
	switch {
	case stoppedMidWork:
		return "I ran out of tool calls before finishing. Ask again with a narrower question, or say which part to continue."
	case presented > 0:
		return "See the table above."
	}
	return "(the model returned no text)"
}

// ClearHistory forgets the conversation so far.
func (r *Runner) ClearHistory() {
	r.mu.Lock()
	r.history = nil
	r.mu.Unlock()
}

// HistoryLen reports how many messages are kept for the next turn.
func (r *Runner) HistoryLen() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.history)
}

func trimHistory(h []fantasy.Message, maxMessages int) []fantasy.Message {
	if len(h) <= maxMessages {
		return h
	}
	return h[len(h)-maxMessages:]
}

func truncateText(s string, maxChars int) string {
	if maxChars <= 0 || len([]rune(s)) <= maxChars {
		return s
	}
	r := []rune(s)
	return string(r[:maxChars]) + "…"
}

// assistantMessage is the counterpart of fantasy.NewUserMessage, which the
// library does not provide.
func assistantMessage(text string) fantasy.Message {
	return fantasy.Message{
		Role:    fantasy.MessageRoleAssistant,
		Content: []fantasy.MessagePart{fantasy.TextPart{Text: text}},
	}
}
