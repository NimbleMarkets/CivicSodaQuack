// Copyright (c) 2026 Neomantra Corp

// Package tui is the Bubble Tea chat window: a transcript of what was said
// and shown, a prompt line, and a few slash commands.
//
// The window talks to the model only through Chat, so a test can drive it
// with a fake and the agent package never learns about terminals.
package tui

import (
	"context"
	"fmt"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/neomantra/CivicSodaQuack/chat/internal/agent"
	"github.com/neomantra/CivicSodaQuack/chat/internal/present"
)

// Chat is what the window needs from the agent.
type Chat interface {
	Ask(ctx context.Context, prompt string, progress agent.ProgressFunc, show present.Func) (agent.Response, error)
	ClearHistory()
	// Model names the vendor and model in use.
	Model() string
}

// ChartView draws a chart into a w×h cell area. The window holds it as an
// interface so it never imports the compiler.
type ChartView interface {
	Render(c present.Chart, w, h int) (view string, warnings []string, err error)
}

// Info is static text for the header and /status.
type Info struct {
	Portals     []string
	SessionPath string
}

type role string

const (
	roleUser      role = "you"
	roleAssistant role = "csq"
	roleSystem    role = "system"
	roleError     role = "error"
)

// entry is one item in the transcript: a message, or a presentation cell.
type entry struct {
	role role
	text string
	cell *cell
}

// cell is a numbered presentation the person can find again by its title.
type cell struct {
	index   int
	created time.Time
	p       present.Presentation
	// rendered caches the cell's view for one width.
	rendered string
	width    int
}

func (c *cell) label() string { return fmt.Sprintf("[%d] %s", c.index, c.p.Title) }

// Model is the window state.
type Model struct {
	ctx    context.Context
	chat   Chat
	charts ChartView
	info   Info
	styles styles

	width, height int
	entries       []entry
	cells         []*cell
	scroll        int // lines scrolled up from the bottom

	input  []rune
	cursor int

	busy      bool
	activity  string
	frame     int
	cancelAsk context.CancelFunc
	progress  <-chan agent.Event
	shows     <-chan present.Presentation
	lastResp  *agent.Response
	quitting  bool
}

// New builds a window over chat. The context bounds every turn; cancelling
// it ends the program.
func New(ctx context.Context, chat Chat, info Info) Model {
	m := Model{ctx: ctx, chat: chat, info: info, styles: newStyles(), width: 80, height: 24}
	m.entries = append(m.entries, entry{role: roleSystem, text: "Ask about the attached data. /help lists commands."})
	return m
}

type responseMsg struct {
	resp agent.Response
	err  error
}

type progressMsg struct {
	ev agent.Event
	ok bool
}

type showMsg struct {
	p  present.Presentation
	ok bool
}

type tickMsg time.Time

// WithCharts lets the window draw chart cells. Without it a chart cell says
// so rather than showing nothing.
func (m Model) WithCharts(v ChartView) Model {
	m.charts = v
	return m
}

// Init satisfies tea.Model. There is no startup work in this slice.
func (m Model) Init() tea.Cmd { return nil }

// Update satisfies tea.Model.
func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = max(40, msg.Width)
		m.height = max(10, msg.Height)
		for _, c := range m.cells {
			c.rendered = ""
		}
		return m, nil

	case tea.KeyPressMsg:
		return m.handleKey(msg)

	case progressMsg:
		if !msg.ok {
			return m, nil
		}
		if msg.ev.Kind == agent.EventProgress && msg.ev.Message != "" {
			m.activity = msg.ev.Message
		}
		return m, waitProgress(m.progress)

	case showMsg:
		if !msg.ok {
			return m, nil
		}
		m.addCell(msg.p)
		m.scroll = 0
		return m, waitShow(m.shows)

	case responseMsg:
		// askCmd closes both channels before this message exists, so anything
		// still queued can be drained here. That keeps a table that was shown
		// during the turn above the reply that comments on it.
		m.drainShows()
		m.busy = false
		m.activity = ""
		m.cancelAsk = nil
		if msg.err != nil {
			m.entries = append(m.entries, entry{role: roleError, text: msg.err.Error()})
		} else {
			r := msg.resp
			m.lastResp = &r
			m.entries = append(m.entries, entry{role: roleAssistant, text: r.Text})
		}
		m.scroll = 0
		return m, nil

	case tickMsg:
		if !m.busy {
			return m, nil
		}
		m.frame++
		return m, tick()
	}
	return m, nil
}

func (m *Model) drainShows() {
	if m.shows == nil {
		return
	}
	for {
		select {
		case p, ok := <-m.shows:
			if !ok {
				m.shows = nil
				return
			}
			m.addCell(p)
		default:
			return
		}
	}
}

func (m *Model) addCell(p present.Presentation) {
	c := &cell{index: len(m.cells) + 1, created: time.Now(), p: p}
	m.cells = append(m.cells, c)
	m.entries = append(m.entries, entry{cell: c})
}

func (m Model) handleKey(msg tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "ctrl+c":
		if m.busy && m.cancelAsk != nil {
			m.cancelAsk()
			m.activity = "cancelling…"
			return m, nil
		}
		m.quitting = true
		return m, tea.Quit
	case "enter":
		return m.submit()
	case "ctrl+l":
		return m.clear()
	case "up", "pgup":
		step := 1
		if msg.String() == "pgup" {
			step = max(1, m.transcriptHeight()-1)
		}
		m.scroll += step
		return m, nil
	case "down", "pgdown":
		step := 1
		if msg.String() == "pgdown" {
			step = max(1, m.transcriptHeight()-1)
		}
		m.scroll = max(0, m.scroll-step)
		return m, nil
	case "home":
		m.scroll = 1 << 20
		return m, nil
	case "end":
		m.scroll = 0
		return m, nil
	case "left":
		m.cursor = max(0, m.cursor-1)
		return m, nil
	case "right":
		m.cursor = min(len(m.input), m.cursor+1)
		return m, nil
	case "ctrl+a":
		m.cursor = 0
		return m, nil
	case "ctrl+e":
		m.cursor = len(m.input)
		return m, nil
	case "backspace":
		if m.cursor > 0 {
			m.input = append(m.input[:m.cursor-1], m.input[m.cursor:]...)
			m.cursor--
		}
		return m, nil
	case "delete":
		if m.cursor < len(m.input) {
			m.input = append(m.input[:m.cursor], m.input[m.cursor+1:]...)
		}
		return m, nil
	case "ctrl+u":
		m.input = m.input[m.cursor:]
		m.cursor = 0
		return m, nil
	case "ctrl+k":
		m.input = m.input[:m.cursor]
		return m, nil
	case "space":
		m.insert(' ')
		return m, nil
	}
	if msg.Text != "" {
		for _, r := range msg.Text {
			m.insert(r)
		}
	}
	return m, nil
}

func (m *Model) insert(r rune) {
	m.input = append(m.input[:m.cursor], append([]rune{r}, m.input[m.cursor:]...)...)
	m.cursor++
}

func (m Model) submit() (tea.Model, tea.Cmd) {
	if m.busy {
		return m, nil
	}
	text := strings.TrimSpace(string(m.input))
	if text == "" {
		return m, nil
	}
	m.input = nil
	m.cursor = 0
	if strings.HasPrefix(text, "/") {
		return m.command(text)
	}

	m.entries = append(m.entries, entry{role: roleUser, text: text})
	m.busy = true
	m.activity = "sending"
	m.scroll = 0
	ctx, cancel := context.WithCancel(m.ctx)
	m.cancelAsk = cancel
	progress := make(chan agent.Event, 32)
	shows := make(chan present.Presentation, 8)
	m.progress = progress
	m.shows = shows
	return m, tea.Batch(askCmd(ctx, m.chat, text, progress, shows), waitProgress(progress), waitShow(shows), tick())
}

func (m Model) clear() (tea.Model, tea.Cmd) {
	if m.busy {
		return m, nil
	}
	m.chat.ClearHistory()
	m.entries = []entry{{role: roleSystem, text: "conversation cleared"}}
	m.cells = nil
	m.lastResp = nil
	m.scroll = 0
	return m, nil
}

func (m Model) command(text string) (tea.Model, tea.Cmd) {
	name := strings.Fields(text)[0]
	switch name {
	case "/quit", "/exit", "/q":
		m.quitting = true
		return m, tea.Quit
	case "/clear":
		return m.clear()
	case "/help", "/?":
		m.entries = append(m.entries, entry{role: roleSystem, text: helpText})
	case "/status":
		m.entries = append(m.entries, entry{role: roleSystem, text: m.statusText()})
	case "/tables", "/cells":
		m.entries = append(m.entries, entry{role: roleSystem, text: m.cellsText()})
	default:
		m.entries = append(m.entries, entry{role: roleSystem, text: "unknown command " + name + "; /help lists commands"})
	}
	m.scroll = 0
	return m, nil
}

const helpText = `Commands:
  /help     this list
  /status   model, portals, session file, last turn
  /tables   list the tables shown this session
  /clear    forget the conversation (also ctrl+l)
  /quit     exit (also ctrl+c when idle)
Keys: enter sends · ↑/↓ and pgup/pgdn scroll the transcript · ctrl+c cancels a running turn`

func (m Model) statusText() string {
	var b strings.Builder
	fmt.Fprintf(&b, "model: %s\nportals: %s\n", m.chat.Model(), strings.Join(m.info.Portals, ", "))
	if m.info.SessionPath != "" {
		fmt.Fprintf(&b, "session: %s\n", m.info.SessionPath)
	}
	if r := m.lastResp; r != nil {
		fmt.Fprintf(&b, "last turn: %d step(s), %d tool call(s), %d shown, %d in / %d out tokens, %s",
			r.Steps, r.ToolCalls, r.Presented, r.Usage.InputTokens, r.Usage.OutputTokens, r.Duration.Round(time.Millisecond))
	} else {
		b.WriteString("last turn: none yet")
	}
	return b.String()
}

func (m Model) cellsText() string {
	if len(m.cells) == 0 {
		return "no tables shown yet"
	}
	lines := make([]string, 0, len(m.cells))
	for _, c := range m.cells {
		s := c.p.Summary()
		lines = append(lines, fmt.Sprintf("%s — %d row(s), %s", c.label(), s.Total, c.created.Format("15:04:05")))
	}
	return strings.Join(lines, "\n")
}

func askCmd(ctx context.Context, chat Chat, prompt string, progress chan<- agent.Event, shows chan<- present.Presentation) tea.Cmd {
	return func() tea.Msg {
		defer close(progress)
		defer close(shows)
		resp, err := chat.Ask(ctx, prompt,
			func(ev agent.Event) {
				select {
				case progress <- ev:
				case <-ctx.Done():
				}
			},
			func(p present.Presentation) {
				select {
				case shows <- p:
				case <-ctx.Done():
				}
			},
		)
		return responseMsg{resp: resp, err: err}
	}
}

func waitProgress(ch <-chan agent.Event) tea.Cmd {
	return func() tea.Msg {
		ev, ok := <-ch
		return progressMsg{ev: ev, ok: ok}
	}
}

func waitShow(ch <-chan present.Presentation) tea.Cmd {
	return func() tea.Msg {
		p, ok := <-ch
		return showMsg{p: p, ok: ok}
	}
}

func tick() tea.Cmd {
	return tea.Tick(150*time.Millisecond, func(t time.Time) tea.Msg { return tickMsg(t) })
}
