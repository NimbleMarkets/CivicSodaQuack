// Copyright (c) 2026 Neomantra Corp

// Package agenttest provides a scripted fantasy.LanguageModel so the agent,
// the window, and the binary can be tested without a provider.
package agenttest

import (
	"context"
	"errors"
	"sync"

	"charm.land/fantasy"
)

// FakeModel plays a script: each Generate call returns the next response and
// records the prompt it was given, so a test can see what the model saw.
type FakeModel struct {
	mu     sync.Mutex
	script []fantasy.Response
	Calls  []fantasy.Call
}

// NewFakeModel returns a model that will answer with script, in order.
func NewFakeModel(script ...fantasy.Response) *FakeModel {
	return &FakeModel{script: script}
}

// Generate implements fantasy.LanguageModel.
func (f *FakeModel) Generate(_ context.Context, call fantasy.Call) (*fantasy.Response, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls = append(f.Calls, call)
	if len(f.script) == 0 {
		return nil, errors.New("fake model: script exhausted")
	}
	r := f.script[0]
	f.script = f.script[1:]
	return &r, nil
}

// Stream implements fantasy.LanguageModel by replaying Generate's answer as
// stream parts.
func (f *FakeModel) Stream(ctx context.Context, call fantasy.Call) (fantasy.StreamResponse, error) {
	r, err := f.Generate(ctx, call)
	if err != nil {
		return nil, err
	}
	return func(yield func(fantasy.StreamPart) bool) {
		for _, c := range r.Content {
			switch c := c.(type) {
			case fantasy.TextContent:
				if !yield(fantasy.StreamPart{Type: fantasy.StreamPartTypeTextDelta, Delta: c.Text}) {
					return
				}
			case fantasy.ToolCallContent:
				if !yield(fantasy.StreamPart{Type: fantasy.StreamPartTypeToolCall, ID: c.ToolCallID, ToolCallName: c.ToolName, ToolCallInput: c.Input}) {
					return
				}
			}
		}
		yield(fantasy.StreamPart{Type: fantasy.StreamPartTypeFinish, FinishReason: r.FinishReason, Usage: r.Usage})
	}, nil
}

// GenerateObject implements fantasy.LanguageModel; structured output is not scripted.
func (f *FakeModel) GenerateObject(context.Context, fantasy.ObjectCall) (*fantasy.ObjectResponse, error) {
	return nil, errors.New("fake model: objects not supported")
}

// StreamObject implements fantasy.LanguageModel; structured output is not scripted.
func (f *FakeModel) StreamObject(context.Context, fantasy.ObjectCall) (fantasy.ObjectStreamResponse, error) {
	return nil, errors.New("fake model: objects not supported")
}

// Provider implements fantasy.LanguageModel.
func (f *FakeModel) Provider() string { return "fake" }

// Model implements fantasy.LanguageModel.
func (f *FakeModel) Model() string { return "scripted" }

var _ fantasy.LanguageModel = (*FakeModel)(nil)

// Text is a final answer.
func Text(text string) fantasy.Response {
	return fantasy.Response{
		Content:      fantasy.ResponseContent{fantasy.TextContent{Text: text}},
		FinishReason: fantasy.FinishReasonStop,
		Usage:        fantasy.Usage{InputTokens: 10, OutputTokens: 5},
	}
}

// ToolCall asks the host to run one tool.
func ToolCall(id, name, input string) fantasy.Response {
	return fantasy.Response{
		Content:      fantasy.ResponseContent{fantasy.ToolCallContent{ToolCallID: id, ToolName: name, Input: input}},
		FinishReason: fantasy.FinishReasonToolCalls,
	}
}

// ToolResultText returns what the model was told about tool call id in
// call's prompt, or "" when there is no such result.
func ToolResultText(call fantasy.Call, id string) string {
	for _, m := range call.Prompt {
		for _, part := range m.Content {
			tr, ok := part.(fantasy.ToolResultPart)
			if !ok || tr.ToolCallID != id {
				continue
			}
			switch out := tr.Output.(type) {
			case fantasy.ToolResultOutputContentText:
				return out.Text
			case fantasy.ToolResultOutputContentError:
				return "ERROR: " + out.Error.Error()
			}
		}
	}
	return ""
}
