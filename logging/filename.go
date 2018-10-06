package logging

import (
	"strings"
	"runtime"
	"github.com/sirupsen/logrus"
	"fmt"
)

type FilenameHook struct{}

func findCaller(skip int) (string, string, int) {
	var (
		pc       uintptr
		file     string
		function string
		line     int
	)
	for i := 0; i < 10; i++ {
		pc, file, line = getCaller(skip + i)
		if !strings.HasPrefix(file, "logrus/") && !strings.HasPrefix(file, "logrus@") {
			break
		}
	}
	if pc != 0 {
		frames := runtime.CallersFrames([]uintptr{pc})
		frame, _ := frames.Next()
		function = frame.Function
	}

	return file, function, line
}

func getCaller(skip int) (uintptr, string, int) {
	pc, file, line, ok := runtime.Caller(skip)
	if !ok {
		return 0, "", 0
	}

	n := 0
	for i := len(file) - 1; i > 0; i-- {
		if file[i] == '/' {
			n += 1
			if n >= 2 {
				file = file[i+1:]
				break
			}
		}
	}
	return pc, file, line
}

func (hook FilenameHook) Levels() []logrus.Level {
	return []logrus.Level{logrus.ErrorLevel, logrus.FatalLevel, logrus.PanicLevel}
}

func (hook FilenameHook) Fire(entry *logrus.Entry) error {
	file, _, line := findCaller(5)
	entry.Data["source"] = fmt.Sprintf("%s:%d", file, line)
	return nil
}
