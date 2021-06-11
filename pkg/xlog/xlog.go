package xlog

import (
	"fmt"
	"path"
	"runtime"
	"strconv"
	"sync"
	"time"
)

// Level type
type Level uint8

// Log Level
const (
	ErrorLevel Level = iota
	WarnLevel
	InfoLevel
	DebugLevel
	TraceLevel
	Disabled
)

// TimeFormat
const (
	TimeFormat         = "2006/01/02 15:04:05.999"
	DefaultAsyncMsgLen = int64(102400)
)

var levelPrefix = [TraceLevel + 1]string{"[E]", "[W]", "[I]", "[D]", "[T]"}
var glog *Logger

// LogOutput implement it and add output to logger.
type LogOutput interface {
	Write(when time.Time, lv Level, msg string) (int, error)
	Destroy()
}

// LogHook implement it and add hook to logger
// msg is pointer can be change
type LogHook interface {
	Hook(lv Level, msg *string) error
}

// Logger call NewLogger to new one
type Logger struct {
	ws      []LogOutput
	hs      []LogHook
	lv      Level
	async   bool
	msgChan chan *logMsg
	sigChan chan string
	mux     *sync.Mutex
	wg      sync.WaitGroup
}

var logMsgPool *sync.Pool

type logMsg struct {
	when time.Time
	lv   Level
	msg  string
}

// NewLogger create new logger
func NewLogger() *Logger {
	return &Logger{
		lv:    TraceLevel,
		async: false,
		mux:   new(sync.Mutex),
	}
}

// SetLogLevel for glog
func SetLogLevel(lv Level) {
	glog.SetLogLevel(lv)
}

// AddOutput for glog
func AddOutput(lo LogOutput) {
	glog.AddOutput(lo)
}

// AddHook for glog
func AddHook(h LogHook) {
	glog.AddHook(h)
}

// Error log by error
func Error(format string, v ...interface{}) {
	glog.Error(format, v...)
}

// Warn log by warn
func Warn(format string, v ...interface{}) {
	glog.Warn(format, v...)
}

// Info log by info
func Info(format string, v ...interface{}) {
	glog.Info(format, v...)
}

// Debug log by debug
func Debug(format string, v ...interface{}) {
	glog.Debug(format, v...)
}

// Trace log by trace
func Trace(format string, v ...interface{}) {
	glog.Trace(format, v...)
}

// Async log, msgLen is buffer len, default is DefaultAsyncMsgLen
func Async(msgLen ...int64) {
	glog.Async(msgLen...)
}

// Flush will block to writer consume the channle buffer
// When use async, you  call call this
func Flush() {
	glog.Flush()
}

// Close will flush log and destory writer
// You may call this before program exit to ensure log flush
func Close() {
	glog.Close()
}

// Error log by error
func (lg *Logger) Error(format string, v ...interface{}) {
	if lg.should(ErrorLevel) {
		lg.writeMsg(ErrorLevel, format, v...)
	}
}

// Warn log by warn
func (lg *Logger) Warn(format string, v ...interface{}) {
	if lg.should(WarnLevel) {
		lg.writeMsg(WarnLevel, format, v...)
	}
}

// Info log by info
func (lg *Logger) Info(format string, v ...interface{}) {
	if lg.should(InfoLevel) {
		lg.writeMsg(InfoLevel, format, v...)
	}
}

// Debug log by debug
func (lg *Logger) Debug(format string, v ...interface{}) {
	if lg.should(DebugLevel) {
		lg.writeMsg(DebugLevel, format, v...)
	}
}

// Trace log by trace
func (lg *Logger) Trace(format string, v ...interface{}) {
	if lg.should(TraceLevel) {
		lg.writeMsg(TraceLevel, format, v...)
	}
}

// SetLogLevel level
func (lg *Logger) SetLogLevel(lv Level) {
	lg.lv = lv
}

// Flush channel buffer
func (lg *Logger) Flush() {
	if lg.async {
		lg.sigChan <- "flush"
		lg.wg.Wait()
		lg.wg.Add(1)
		return
	}
}

// Close flush and destory writers
func (lg *Logger) Close() {
	if lg.async {
		lg.sigChan <- "close"
		lg.wg.Wait()
		close(lg.msgChan)
		close(lg.sigChan)
	}
	for _, w := range lg.ws {
		w.Destroy()
	}
}

// Async init async resource and start asnyc routine
func (lg *Logger) Async(msgLen ...int64) {
	lg.mux.Lock()
	defer lg.mux.Unlock()
	if lg.async {
		return
	}
	n := DefaultAsyncMsgLen
	if len(msgLen) > 0 && msgLen[0] > 0 {
		n = msgLen[0]
	}
	lg.msgChan = make(chan *logMsg, n)
	lg.sigChan = make(chan string)
	logMsgPool = &sync.Pool{
		New: func() interface{} {
			return &logMsg{}
		},
	}

	lg.wg.Add(1)
	go lg.startAsync()
	lg.async = true
}

func (lg *Logger) startAsync() {
	gameOver := false
	for {
		select {
		case msg := <-lg.msgChan:
			lg.writeOutput(msg.when, msg.lv, msg.msg)
			logMsgPool.Put(msg)
		case sig := <-lg.sigChan:
			lg.flush()
			if sig == "close" {
				gameOver = true
			}
			lg.wg.Done()
		}
		if gameOver {
			break
		}
	}
}

func (lg *Logger) flush() {
	for {
		if len(lg.msgChan) > 0 {
			msg := <-lg.msgChan
			lg.writeOutput(msg.when, msg.lv, msg.msg)
		} else {
			break
		}
	}
}

// AddOutput add output interface
func (lg *Logger) AddOutput(w LogOutput) {
	lg.ws = append(lg.ws, w)
	if len(lg.ws) > 5 {
		panic(fmt.Errorf("Too many log output"))
	}
}

// AddHook add hook interface
func (lg *Logger) AddHook(h LogHook) {
	lg.hs = append(lg.hs, h)
}

func (lg *Logger) should(lv Level) bool {
	if lg.lv >= lv {
		return true
	}
	return false
}

func (lg *Logger) writeMsg(lv Level, format string, v ...interface{}) {
	when := time.Now()
	msg := fmt.Sprintf(format, v...) + "\n"

	if len(lg.hs) > 0 {
		lg.hs[0].Hook(lv, &msg)
		if len(lg.hs) > 1 {
			for _, h := range lg.hs[:1] {
				h.Hook(lv, &msg)
			}
		}
	}

	_, file, line, _ := runtime.Caller(3)
	msg = " [" + path.Base(file) + ":" + strconv.Itoa(line) + "] " + msg

	if lg.async {
		lm := logMsgPool.Get().(*logMsg)
		lm.lv = lv
		lm.msg = msg
		lm.when = when
		lg.msgChan <- lm
		// if chan is full, write direct
		// select {
		// case lg.msgChan <- lm:
		// 	break
		// default:
		// 	lg.writeOutput(when, lv, msg)
		// 	fmt.Println("chan is full")
		// }
	} else {
		lg.writeOutput(when, lv, msg)
	}

}

func (lg *Logger) writeOutput(when time.Time, lv Level, msg string) {
	if len(lg.ws) > 0 {
		lg.ws[0].Write(when, lv, msg)
		if len(lg.ws) > 1 {
			for _, w := range lg.ws[1:] {
				w.Write(when, lv, msg)
			}
		}
	}
}

func formatTime(when time.Time) string {
	result := when.Format(TimeFormat)
	lwhen := 23 - len(result)
	if lwhen > 0 {
		for i := 0; i < lwhen; i++ {
			result = result + "0"
		}
	}
	return result
}

func init() {
	glog = NewLogger()
}
