// Copyright (c) 2026 Neomantra Corp

package agent

import (
	"context"
	"fmt"
	"os"
	"strings"

	"charm.land/fantasy"
	"charm.land/fantasy/providers/anthropic"
	"charm.land/fantasy/providers/google"
	"charm.land/fantasy/providers/openai"
	"charm.land/fantasy/providers/openaicompat"
	"charm.land/fantasy/providers/openrouter"
)

// ParseModel splits "vendor/model" into its parts. A model with no vendor
// returns an empty vendor; the caller decides what that means.
func ParseModel(model string) (vendor, id string) {
	model = strings.TrimSpace(model)
	i := strings.IndexByte(model, '/')
	if i < 0 {
		return "", model
	}
	v := strings.ToLower(model[:i])
	switch v {
	case "anthropic", "openai", "google", "openrouter", "compat", "openai-compat":
		return v, model[i+1:]
	}
	return "", model
}

// keyEnv names the environment variable each vendor reads when no --api-key
// is given.
var keyEnv = map[string]string{
	"anthropic":     "ANTHROPIC_API_KEY",
	"openai":        "OPENAI_API_KEY",
	"google":        "GEMINI_API_KEY",
	"openrouter":    "OPENROUTER_API_KEY",
	"compat":        "OPENAI_API_KEY",
	"openai-compat": "OPENAI_API_KEY",
}

// NewLanguageModel builds the Fantasy model named by opts.Model.
//
// The vendor prefix picks the provider. "compat/<model>" with --base-url
// talks to any OpenAI-compatible server, including a local Kronk server. A
// bare model name is an error: this slice has no embedded runtime.
func NewLanguageModel(ctx context.Context, opts Options) (fantasy.LanguageModel, error) {
	vendor, id := ParseModel(opts.Model)
	if vendor == "" {
		return nil, fmt.Errorf("model %q needs a vendor prefix: anthropic/, openai/, google/, openrouter/, or compat/ with --base-url", opts.Model)
	}
	if id == "" {
		return nil, fmt.Errorf("model %q names a vendor but no model", opts.Model)
	}
	key := opts.APIKey
	if key == "" {
		key = os.Getenv(keyEnv[vendor])
	}

	var (
		p   fantasy.Provider
		err error
	)
	switch vendor {
	case "anthropic":
		var o []anthropic.Option
		if key != "" {
			o = append(o, anthropic.WithAPIKey(key))
		}
		if opts.BaseURL != "" {
			o = append(o, anthropic.WithBaseURL(opts.BaseURL))
		}
		p, err = anthropic.New(o...)
	case "openai":
		var o []openai.Option
		if key != "" {
			o = append(o, openai.WithAPIKey(key))
		}
		if opts.BaseURL != "" {
			o = append(o, openai.WithBaseURL(opts.BaseURL))
		}
		p, err = openai.New(o...)
	case "google":
		var o []google.Option
		if key != "" {
			o = append(o, google.WithGeminiAPIKey(key))
		}
		if opts.BaseURL != "" {
			o = append(o, google.WithBaseURL(opts.BaseURL))
		}
		p, err = google.New(o...)
	case "openrouter":
		var o []openrouter.Option
		if key != "" {
			o = append(o, openrouter.WithAPIKey(key))
		}
		p, err = openrouter.New(o...)
	case "compat", "openai-compat":
		if opts.BaseURL == "" {
			return nil, fmt.Errorf("compat/%s needs --base-url (or CSQ_CHAT_BASE_URL) pointing at an OpenAI-compatible server", id)
		}
		o := []openaicompat.Option{openaicompat.WithBaseURL(opts.BaseURL)}
		if key != "" {
			o = append(o, openaicompat.WithAPIKey(key))
		}
		p, err = openaicompat.New(o...)
	}
	if err != nil {
		return nil, fmt.Errorf("%s provider: %w", vendor, err)
	}
	m, err := p.LanguageModel(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("%s model %q: %w", vendor, id, err)
	}
	return m, nil
}
