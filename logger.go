package raft

import (
	"github.com/sirupsen/logrus"
	"github.com/x-cray/logrus-prefixed-formatter"
	"os"
	"runtime"
	"fmt"
	"strings"
)

type filenameHook struct{}

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

func (hook filenameHook) Levels() []logrus.Level {
	return []logrus.Level{logrus.ErrorLevel, logrus.FatalLevel, logrus.PanicLevel}
}

func (hook filenameHook) Fire(entry *logrus.Entry) error {
	file, _, line := findCaller(5)
	entry.Data["source"] = fmt.Sprintf("%s:%d", file, line)
	return nil
}

type raftStateHook struct {
	termGetter func() uint64
	nodeGetter func() string
}

func (hook raftStateHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

func (hook raftStateHook) Fire(entry *logrus.Entry) error {
	entry.Data["node"] = hook.nodeGetter()
	entry.Data["term"] = hook.termGetter()
	return nil
}

func (r *RaftNode) setupLogger() {
	formatter := &prefixed.TextFormatter{}
	formatter.TimestampFormat = "2006-01-02 15:04:05.000"
	formatter.FullTimestamp = true

	logger := logrus.New()
	if r.config.VerboseLog {
		logger.SetLevel(logrus.DebugLevel)
	} else {
		logger.SetLevel(logrus.InfoLevel)
	}
	logger.SetFormatter(formatter)
	logger.SetOutput(os.Stdout)

	stateHook := &raftStateHook{
		termGetter: func() uint64 {
			return r.currentTerm
		},
		nodeGetter: func() string {
			return r.localID
		},
	}
	logger.AddHook(&filenameHook{})
	logger.AddHook(stateHook)
	r.logger = logger
}
