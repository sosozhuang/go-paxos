package logger

import (
	"fmt"
	"github.com/Sirupsen/logrus"
	"os"
	"time"
	"sync"
	"io/ioutil"
	"log"
	"github.com/sosozhuang/paxos/util"
)

const (
	CALL_DEPTH = 2
)

type Logger interface {
	Debug(v ...interface{})
	Debugf(format string, v ...interface{})

	Error(v ...interface{})
	Errorf(format string, v ...interface{})

	Info(v ...interface{})
	Infof(format string, v ...interface{})

	Warning(v ...interface{})
	Warningf(format string, v ...interface{})

	Fatal(v ...interface{})
	Fatalf(format string, v ...interface{})

	Panic(v ...interface{})
	Panicf(format string, v ...interface{})
}


var (
	ProposerLogger Logger
	AcceptorLogger Logger
	LearnerLogger  Logger
	PaxosLogger  Logger
	once sync.Once
)

func NewLogger(output, logDir, level string) (Logger, error) {
	return newLogrusLogger(output, logDir, level)
}

func newLogrusLogger(output, logDir, level string) (*logrus.Logger, error) {
	l := logrus.New()
	formatter := &logrus.TextFormatter{DisableSorting: true}
	if output == "stderr" {
		l.Out = os.Stderr
	} else if output == "stdout" {
		l.Out = os.Stdout
	} else if output == "discard" {
		l.Out = ioutil.Discard
	} else {
		formatter.DisableColors = true
		if out, err := touchLogOutput(output, logDir, true); err != nil {
			return nil, err
		} else {
			util.AddHook(func() {out.Close()})
			l.Out = out
		}
	}
	l.Formatter = formatter
	if lvl, err := logrus.ParseLevel(level); err == nil {
		l.Level = lvl
	}
	return l, nil
}

func SetupLogger(output, logDir, level string) error {
	var err error
	once.Do(func() {
		if output != "" {
			var l *logrus.Logger
			l, err = newLogrusLogger(output, logDir, level)
			if err != nil {
				return
			}
			ProposerLogger = l.WithField("e", "proposer")
			AcceptorLogger = l.WithField("e", "acceptor")
			LearnerLogger = l.WithField("e", "learner")
			PaxosLogger = l.WithField("e", "paxos")
		} else {
			ProposerLogger, err = newLogrusLogger("proposer.log", logDir, level)
			if err != nil {
				return
			}
			AcceptorLogger, err = newLogrusLogger("acceptor.log", logDir, level)
			if err != nil {
				return
			}
			LearnerLogger, err = newLogrusLogger("learner.log", logDir, level)
			if err != nil {
				return
			}
			PaxosLogger, err = newLogrusLogger("paxos.log", logDir, level)
			if err != nil {
				return
			}
		}
	})
	return err
}

func logFileName(output string) string {
	return fmt.Sprintf("%s.%s", output, time.Now().Format("2006-01-02T15:04:05"))
}

func touchLogOutput(output, logDir string, append bool) (*os.File, error) {
	if output == "" {
		return os.Stdout, nil
	}
	if logDir == "" {
		logDir = "."
	} else {
		info, err := os.Stat(logDir)
		if err == nil {
			if !info.IsDir() {
				return nil, fmt.Errorf("%s is not a directory", logDir)
			}
		} else if os.IsNotExist(err) {
			if err = os.MkdirAll(logDir, 0755); err != nil {
				return nil, err
			}
		}
	}
	var f *os.File
	info, err := os.Stat(logDir + string(os.PathSeparator) + output)
	if err == nil {
		if info.IsDir() {
			//name = name + string(os.PathSeparator) + "component.log"
			return nil, fmt.Errorf("%s is a directory", output)
		} else {
			var flag int
			if append {
				flag = os.O_RDWR | os.O_APPEND
			} else {
				flag = os.O_RDWR | os.O_TRUNC
			}
			f, err = os.OpenFile(output, flag, 0)
		}
	} else if os.IsNotExist(err) {
		f, err = os.Create(output)
	}
	return f, err
}

func newDefaultLogger(output, logDir, prefix, level string) (Logger, error) {
	l := &defaultLogger{}
	if output == "stderr" {
		l.Logger = log.New(os.Stderr, prefix, log.LstdFlags)
	} else if output == "stdout" {
		l.Logger = log.New(os.Stdout, prefix, log.LstdFlags)
	} else if output == "discard" {
		l.Logger = log.New(ioutil.Discard, prefix, log.LstdFlags)
	} else {
		if out, err := touchLogOutput(output, logDir, true); err != nil {
			return nil, err
		} else {
			//todo: close log output
			//func () {
			//	out.Close()
			//}
			l.Logger = log.New(out, prefix, log.LstdFlags)
		}
	}
	if level == "debug" {
		l.debug = true
		l.SetFlags(log.LstdFlags | log.Lshortfile)
	}
	return l, nil
}

type defaultLogger struct {
	*log.Logger
	debug bool
}

func (l *defaultLogger) Debug(v ...interface{}) {
	if l.debug {
		l.Output(CALL_DEPTH, header("DEBUG", fmt.Sprint(v...)))
	}
}

func (l *defaultLogger) Debugf(format string, v ...interface{}) {
	if l.debug {
		l.Output(CALL_DEPTH, header("DEBUG", fmt.Sprintf(format, v...)))
	}
}

func (l *defaultLogger) Info(v ...interface{}) {
	l.Output(CALL_DEPTH, header("INFO", fmt.Sprint(v...)))
}

func (l *defaultLogger) Infof(format string, v ...interface{}) {
	l.Output(CALL_DEPTH, header("INFO", fmt.Sprintf(format, v...)))
}

func (l *defaultLogger) Error(v ...interface{}) {
	l.Output(CALL_DEPTH, header("ERROR", fmt.Sprint(v...)))
}

func (l *defaultLogger) Errorf(format string, v ...interface{}) {
	l.Output(CALL_DEPTH, header("ERROR", fmt.Sprintf(format, v...)))
}

func (l *defaultLogger) Warning(v ...interface{}) {
	l.Output(CALL_DEPTH, header("WARN", fmt.Sprint(v...)))
}

func (l *defaultLogger) Warningf(format string, v ...interface{}) {
	l.Output(CALL_DEPTH, header("WARN", fmt.Sprintf(format, v...)))
}

func (l *defaultLogger) Fatal(v ...interface{}) {
	l.Output(CALL_DEPTH, header("FATAL", fmt.Sprint(v...)))
	os.Exit(1)
}

func (l *defaultLogger) Fatalf(format string, v ...interface{}) {
	l.Output(CALL_DEPTH, header("FATAL", fmt.Sprintf(format, v...)))
	os.Exit(1)
}

func (l *defaultLogger) Panic(v ...interface{}) {
	l.Logger.Panic(v)
}

func (l *defaultLogger) Panicf(format string, v ...interface{}) {
	l.Logger.Panicf(format, v...)
}

func header(lvl, msg string) string {
	return fmt.Sprintf("%s: %s", lvl, msg)
}