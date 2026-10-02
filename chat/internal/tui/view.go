// Copyright (c) 2026 Neomantra Corp

package tui

import (
	"fmt"
	"strings"

	"charm.land/bubbles/v2/table"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
)

type styles struct {
	header    lipgloss.Style
	divider   lipgloss.Style
	input     lipgloss.Style
	label     lipgloss.Style
	user      lipgloss.Style
	assistant lipgloss.Style
	system    lipgloss.Style
	err       lipgloss.Style
	meta      lipgloss.Style
	cursor    lipgloss.Style
}

func newStyles() styles {
	return styles{
		header:    lipgloss.NewStyle().Foreground(lipgloss.Color("230")).Background(lipgloss.Color("24")).Bold(true).Padding(0, 1),
		divider:   lipgloss.NewStyle().Foreground(lipgloss.Color("240")),
		input:     lipgloss.NewStyle().Foreground(lipgloss.Color("231")).Background(lipgloss.Color("235")).Padding(0, 1),
		label:     lipgloss.NewStyle().Foreground(lipgloss.Color("79")).Bold(true),
		user:      lipgloss.NewStyle().Foreground(lipgloss.Color("230")),
		assistant: lipgloss.NewStyle().Foreground(lipgloss.Color("159")),
		system:    lipgloss.NewStyle().Foreground(lipgloss.Color("244")),
		err:       lipgloss.NewStyle().Foreground(lipgloss.Color("203")),
		meta:      lipgloss.NewStyle().Foreground(lipgloss.Color("244")),
		cursor:    lipgloss.NewStyle().Reverse(true),
	}
}

// View satisfies tea.Model.
func (m Model) View() tea.View {
	v := tea.NewView(m.render())
	v.AltScreen = true
	v.WindowTitle = "csq-chat"
	return v
}

// render is the whole screen as a string, which tests read directly.
func (m Model) render() string {
	width := max(20, m.width-2)
	header := m.renderHeader(width)
	footer := m.renderInput(width)
	body := m.renderTranscript(width, m.transcriptHeight())
	return lipgloss.JoinVertical(lipgloss.Left, header, body, footer)
}

// transcriptHeight is the rows left between header and footer.
func (m Model) transcriptHeight() int {
	return max(1, m.height-1-3)
}

func (m Model) renderHeader(width int) string {
	left := "csq-chat"
	if m.busy {
		spinner := []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}
		left = spinner[m.frame%len(spinner)] + " working"
	}
	right := m.chat.Model() + " · " + strings.Join(m.info.Portals, ", ")
	gap := width - 2 - lipgloss.Width(left) - lipgloss.Width(right)
	if gap < 1 {
		right = truncate(right, max(0, width-2-lipgloss.Width(left)-1))
		gap = 1
	}
	return m.styles.header.Width(width).Render(left + strings.Repeat(" ", gap) + right)
}

func (m Model) renderTranscript(width, height int) string {
	var lines []string
	for _, e := range m.entries {
		if e.cell != nil {
			lines = append(lines, strings.Split(m.renderCell(e.cell, width), "\n")...)
		} else {
			lines = append(lines, m.renderEntry(e, width)...)
		}
		lines = append(lines, "")
	}
	if m.busy {
		activity := m.activity
		if activity == "" {
			activity = "waiting for the model"
		}
		lines = append(lines, m.styles.meta.Render(truncate(activity, width)))
	}
	if len(lines) == 0 {
		lines = []string{""}
	}
	start := max(0, len(lines)-height-m.scroll)
	end := min(len(lines), start+height)
	visible := lines[start:end]
	for len(visible) < height {
		visible = append(visible, "")
	}
	return strings.Join(visible, "\n")
}

func (m Model) renderEntry(e entry, width int) []string {
	style := m.styles.system
	switch e.role {
	case roleUser:
		style = m.styles.user
	case roleAssistant:
		style = m.styles.assistant
	case roleError:
		style = m.styles.err
	}
	label := m.styles.label.Render(string(e.role) + ":")
	var out []string
	for i, line := range strings.Split(wrap(e.text, max(10, width-6)), "\n") {
		if i == 0 {
			out = append(out, label+" "+style.Render(line))
		} else {
			out = append(out, strings.Repeat(" ", lipgloss.Width(string(e.role))+2)+style.Render(line))
		}
	}
	return out
}

func (m Model) renderInput(width int) string {
	prompt := "> "
	if m.busy {
		prompt = "… "
	}
	rendered := prompt + string(m.input)
	if !m.busy {
		// The cursor highlights the rune under it rather than replacing it.
		before := string(m.input[:m.cursor])
		under, after := " ", ""
		if m.cursor < len(m.input) {
			under = string(m.input[m.cursor])
			after = string(m.input[m.cursor+1:])
		}
		rendered = prompt + before + m.styles.cursor.Render(under) + after
	}
	hint := "enter sends · ↑/↓ scroll · /help"
	if m.busy {
		hint = "working — ctrl+c cancels"
	}
	return lipgloss.JoinVertical(lipgloss.Left,
		m.styles.divider.Render(strings.Repeat("─", width)),
		m.styles.input.Width(width).Render(rendered),
		m.styles.meta.Width(width).Render(hint),
	)
}

// renderCell draws a presentation. A chart that cannot be drawn says why in
// place, so a person is never left wondering whether something was meant to
// appear.
func (m Model) renderCell(c *cell, width int) string {
	if c.rendered != "" && c.width == width {
		return c.rendered
	}
	var out string
	switch {
	case c.p.Table != nil:
		out = m.renderTable(c, width)
	case c.p.Chart != nil:
		out = m.renderChart(c, width)
	default:
		out = m.styles.meta.Render(fmt.Sprintf("[%s] %s", c.p.Kind, c.label()))
	}
	c.rendered, c.width = out, width
	return out
}

// chartHeight is how many rows a chart cell takes: tall enough to read, short
// enough that the reply beneath it stays on screen.
func (m Model) chartHeight() int {
	return min(18, max(8, m.transcriptHeight()-4))
}

func (m Model) renderChart(c *cell, width int) string {
	title := m.styles.label.Render("▤ " + c.label())
	s := c.p.Summary()
	if m.charts == nil {
		return lipgloss.JoinVertical(lipgloss.Left, title,
			m.styles.meta.Render(fmt.Sprintf("chart over %d row(s) — no chart renderer is attached", s.Total)))
	}
	view, warnings, err := m.charts.Render(*c.p.Chart, width, m.chartHeight())
	if err != nil {
		return lipgloss.JoinVertical(lipgloss.Left, title, m.styles.err.Render("chart could not be drawn: "+err.Error()))
	}
	parts := []string{title, view}
	footer := fmt.Sprintf("%d row(s)", s.Total)
	if s.Truncated {
		footer += " (query hit its row cap; the chart shows part of the data)"
	}
	parts = append(parts, m.styles.meta.Render(footer))
	for _, w := range warnings {
		parts = append(parts, m.styles.meta.Render("note: "+truncate(w, max(10, width-6))))
	}
	return lipgloss.JoinVertical(lipgloss.Left, parts...)
}

const maxCellRows = 500

func (m Model) renderTable(c *cell, width int) string {
	t := c.p.Table
	title := m.styles.label.Render("▦ " + c.label())
	if len(t.Columns) == 0 {
		return lipgloss.JoinVertical(lipgloss.Left, title, m.styles.meta.Render("(no columns)"))
	}
	cols := fitColumns(t.Columns, t.Rows, width)
	rows := make([]table.Row, 0, len(t.Rows))
	for i, r := range t.Rows {
		if i >= maxCellRows {
			break
		}
		rows = append(rows, table.Row(r))
	}
	st := table.DefaultStyles()
	st.Header = st.Header.Foreground(lipgloss.Color("230")).Background(lipgloss.Color("24")).Bold(true)
	st.Selected = lipgloss.NewStyle()
	tbl := table.New(table.WithColumns(cols), table.WithStyles(st), table.WithWidth(width))
	tbl.SetRows(rows)
	tbl.SetHeight(max(1, len(rows)) + 1)

	footer := fmt.Sprintf("%d row(s)", t.Total)
	if t.Truncated || len(t.Rows) < t.Total {
		footer = fmt.Sprintf("%d of %d row(s)", len(rows), t.Total)
	}
	return lipgloss.JoinVertical(lipgloss.Left, title, tbl.View(), m.styles.meta.Render(footer))
}

// fitColumns sizes columns to their content, then shrinks the widest one
// cell at a time until the table fits, so narrow numeric columns keep their
// full width while long text columns give way.
func fitColumns(headers []string, rows [][]string, avail int) []table.Column {
	const (
		maxCol     = 40
		minContent = 3
		pad        = 2
	)
	n := len(headers)
	widths := make([]int, n)
	for i, h := range headers {
		widths[i] = lipgloss.Width(h)
	}
	for _, r := range rows {
		for i := 0; i < n && i < len(r); i++ {
			if w := lipgloss.Width(r[i]); w > widths[i] {
				widths[i] = w
			}
		}
	}
	total := 0
	for i := range widths {
		widths[i] = min(max(widths[i], minContent), maxCol)
		total += widths[i]
	}
	usable := max(avail-pad*n, n)
	for total > usable {
		wi := -1
		for i := range widths {
			if widths[i] > minContent && (wi < 0 || widths[i] > widths[wi]) {
				wi = i
			}
		}
		if wi < 0 {
			break
		}
		widths[wi]--
		total--
	}
	out := make([]table.Column, n)
	for i, h := range headers {
		out[i] = table.Column{Title: h, Width: widths[i]}
	}
	return out
}

func wrap(s string, width int) string {
	var out []string
	for _, para := range strings.Split(s, "\n") {
		words := strings.Fields(para)
		if len(words) == 0 {
			out = append(out, "")
			continue
		}
		line := words[0]
		for _, w := range words[1:] {
			if lipgloss.Width(line)+1+lipgloss.Width(w) > width {
				out = append(out, line)
				line = w
				continue
			}
			line += " " + w
		}
		out = append(out, line)
	}
	return strings.Join(out, "\n")
}

func truncate(s string, width int) string {
	if lipgloss.Width(s) <= width {
		return s
	}
	r := []rune(s)
	for len(r) > 0 && lipgloss.Width(string(r))+1 > width {
		r = r[:len(r)-1]
	}
	return string(r) + "…"
}
