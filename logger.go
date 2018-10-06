package raft

import (
	"github.com/sirupsen/logrus"
	"os"
	"github.com/dzdx/raft/logging"
	"github.com/x-cray/logrus-prefixed-formatter"
	"fmt"
	"bytes"
)

type raftStateHook struct {
	termGetter  func() uint64
	nodeGetter  func() string
	stateGetter func() string
}

func (hook raftStateHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

func (hook raftStateHook) Fire(entry *logrus.Entry) error {
	var buf bytes.Buffer
	if prefix, ok := entry.Data["prefix"]; ok {
		buf.WriteString(prefix.(string))
	}
	raftPrefix := fmt.Sprintf("n%-2s Term:%-3d %-12s", hook.nodeGetter(), hook.termGetter(), hook.stateGetter())
	buf.WriteString(raftPrefix)
	entry.Data["prefix"] = buf.String()
	return nil
}

func (r *RaftNode) setupLogger() {
	formatter := &prefixed.TextFormatter{
		ForceFormatting: true,
		TimestampFormat: "2006-01-02 15:04:05.000",
		FullTimestamp:   true,
	}
	formatter.SetColorScheme(&prefixed.ColorScheme{TimestampStyle: "green"})

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
		stateGetter: func() string {
			return map[State]string{
				Follower:  "follower",
				Candidate: "candidate",
				Leader:    "leader",
			}[r.state]
		},
	}
	logger.AddHook(&logging.FilenameHook{})
	logger.AddHook(stateHook)
	r.logger = logger
}
