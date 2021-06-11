package xlog

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// default data
const (
	FileRotateSize  = 1024 * 1024 * 500
	FileRotateCount = 5
	DateFormat      = "2006-01-02" // DateFormat ...
)

// rotate type
const (
	RotateNull = iota
	RotateDate
	RotateSize
)

// FileWriter implement interface LogOutput
type FileWriter struct {
	// config meta
	FilePath    string `json:"filepath"`
	FileName    string `json:"filename"`
	RotateSize  uint64 `json:"rotatesize"`
	RotateCount int16  `json:"rotatecount"`
	// contorl meta
	mux      *sync.Mutex
	file     *os.File  // current file fd
	fileDate time.Time // current file date
	curName  string    // current full name
	curSize  int64     // current file size
}

var (
	gwriter *FileWriter
	once    sync.Once
)

// SetLogFilePath set log file path for glog
// It just adapte for old xylog, the new use NewLogger(config)
func SetLogFilePath(path, file string) {
	once.Do(func() {
		gwriter.FilePath = path
		gwriter.FileName = file
		err := gwriter.createNewFile()
		if err != nil {
			fmt.Printf("Create File Fail. %s\n", err.Error())
		} else {
			glog.AddOutput(gwriter)
		}
	})
}

// SetLogRotateFileSize set log rotate size
// This fuction not add writer, you must call SetLogFilePath
// It just adapte for old xylog, the new use NewLogger(config)
func SetLogRotateFileSize(size int64) {
	gwriter.RotateSize = uint64(size)
}

func (w *FileWriter) Write(when time.Time, lv Level, msg string) (int, error) {
	w.checkFile(when)
	b := w.formatMsg(when, lv, msg)
	w.mux.Lock()
	defer w.mux.Unlock()
	n, err := w.file.Write(b)
	if err != nil {
		return n, err
	}
	w.curSize += int64(n)
	return n, nil
}

// Destroy close file
func (w *FileWriter) Destroy() {
	w.file.Close()
}

func (w *FileWriter) formatMsg(when time.Time, lv Level, msg string) []byte {
	// _, file, line, _ := runtime.Caller(2)
	// return []byte(fmt.Sprintf("%s %s [%s:%d] %s",
	// 	when.Format(TimeFormat),
	// 	levelPrefix[lv],
	// 	path.Base(file), line,
	// 	msg))
	// whenFormat := when.Format(TimeFormat)
	// lwhen := 23 - len(whenFormat)
	// if lwhen > 0 {
	// 	for i := 0; i < lwhen; i++ {
	// 		whenFormat = whenFormat + "0"
	// 	}
	// }
	return []byte(formatTime(when) + " " + levelPrefix[lv] + msg)
}

func (w *FileWriter) checkFile(when time.Time) {
	rotate := w.checkRotate(when)
	switch rotate {
	case RotateDate:
		w.createNewFile()
	case RotateSize:
		w.rotateNewFile()
	default:
		return
	}
}

func (w *FileWriter) checkRotate(when time.Time) int {
	if w.checkRotateTime(when) {
		return RotateDate
	}
	if w.checkRotateSize() {
		return RotateSize
	}
	return RotateNull
}

func (w *FileWriter) checkRotateTime(when time.Time) bool {
	ts, _ := time.Parse(DateFormat, when.Format(DateFormat))
	return ts.After(w.fileDate)
}

func (w *FileWriter) checkRotateSize() bool {
	return uint64(w.curSize) > w.RotateSize
}

func (w *FileWriter) rotateNewFile() error {

	w.mux.Lock()
	defer w.mux.Unlock()

	// check is conflict with rotateTime
	if !w.checkRotateSize() {
		return fmt.Errorf("Please check rotate confilct")
	}
	if w.file == nil {
		return fmt.Errorf("File is nil when rotate")
	}

	for i := w.RotateCount - 1; i >= 0; i-- {
		oldName := fmt.Sprintf("%s.%d", w.curName, i)
		newName := fmt.Sprintf("%s.%d", w.curName, i+1)
		_, err := os.Stat(oldName)
		if err != nil {
			continue
		}
		err = os.Rename(oldName, newName)
		if err != nil {
			fmt.Println("Rename error.", err.Error())
		}
	}

	w.file.Close()
	err := os.Rename(w.curName, fmt.Sprintf("%s.%d", w.curName, 0))
	if err != nil {
		return fmt.Errorf("Rename error. %s", err.Error())
	}
	file, err := os.OpenFile(w.curName, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		return fmt.Errorf("OpenFile error. %s", err.Error())
	}
	w.file = file
	w.curSize = 0

	return nil
}

func (w *FileWriter) createNewFile() error {
	w.mux.Lock()
	defer w.mux.Unlock()

	// check is conflict with rotatesize
	if !w.checkRotateTime(time.Now()) {
		return fmt.Errorf("Please check rotate confilct")
	}
	if w.file != nil {
		w.file.Close()
	}

	now, _ := time.Parse(DateFormat, time.Now().Format(DateFormat))
	today := fmt.Sprintf("%.4d%.2d%.2d", now.Year(), now.Month(), now.Day())
	w.curName = fmt.Sprintf("%s/%s.%s", w.FilePath, w.FileName, today)
	file, err := os.OpenFile(w.curName, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		return err
	}

	w.file = file
	w.fileDate = now
	w.curSize = GetFileSize(w.curName)
	return nil
}

// GetFileSize for check file size
func GetFileSize(file string) int64 {
	f, e := os.Stat(file)
	if e != nil {
		fmt.Println(e.Error())
		return 0
	}
	return f.Size()
}

// NewFileWriter will create new file
func NewFileWriter(config string) (*FileWriter, error) {
	w := &FileWriter{
		FilePath:    "./",
		FileName:    "default.log",
		RotateSize:  FileRotateSize,
		RotateCount: FileRotateCount,
		mux:         new(sync.Mutex),
	}
	err := json.Unmarshal([]byte(config), w)
	if err != nil {
		return nil, fmt.Errorf("Invalid ")
	}
	err = w.createNewFile()
	if err != nil {
		return nil, err
	}
	return w, nil
}

func init() {
	gwriter = &FileWriter{
		FilePath:    "./",
		FileName:    "default.log",
		RotateSize:  FileRotateSize,
		RotateCount: FileRotateCount,
		mux:         new(sync.Mutex),
	}
}
