package xlog

import (
	"fmt"
	"io"
	"os"
	"time"
)

// ConsoleWriter impleme LogOutput interface
type ConsoleWriter struct {
	Color bool `json:"color"`
	lg    io.Writer
}

// brush is a color join function
type brush func(string) string

// newBrush return a fix color Brush
func newBrush(color string) brush {
	pre := "\033["
	reset := "\033[0m"
	return func(text string) string {
		return pre + color + "m" + text + reset
	}
}

var colors = []brush{
	newBrush("1;31"), // Error              red
	newBrush("1;33"), // Warning            yellow
	newBrush("1;35"), // Informational      Background blue
	newBrush("1;34"), // Debug      		blue
	newBrush("1;32"), // Notice             green
}

func (w *ConsoleWriter) formatMsg(when time.Time, lv Level, msg string) []byte {
	// _, file, line, _ := runtime.Caller(6)
	// return []byte(fmt.Sprintf("%s %s [%s:%d] %s",
	// 	when.Format(TimeFormat),
	// 	levelPrefix[lv],
	// 	path.Base(file), line,
	// 	msg))
	return []byte(fmt.Sprintf("%s %s %s",
		formatTime(when),
		//when.Format(TimeFormat),
		levelPrefix[lv],
		msg))
}

func (w *ConsoleWriter) Write(when time.Time, lv Level, msg string) (int, error) {
	if w.Color {
		msg = colors[lv](msg)
	}
	b := w.formatMsg(when, lv, msg)
	n, e := w.lg.Write(b)
	return n, e
}

// Destroy ~
func (w *ConsoleWriter) Destroy() {
}

// NewConsoleWriter todo: add config
func NewConsoleWriter() *ConsoleWriter {
	w := &ConsoleWriter{
		Color: true,
		lg:    os.Stdout,
	}
	return w
}
