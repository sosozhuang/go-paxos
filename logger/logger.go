// Copyright © 2017 sosozhuang <sosozhuang@163.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package logger

import (
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/sosozhuang/paxos/util"
	"io/ioutil"
	"log"
	"os"
	"path"
	"sync"
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
	paxosLogger = logrus.New()
	dLogger     = &defaultLogger{}
	once        sync.Once
)

func NewLogger(output, logDir, level string) (Logger, error) {
	l := logrus.New()
	if err := setupLogger(l, output, logDir, level, true); err != nil {
		return nil, err
	}
	return l, nil
}

func setupLogger(l *logrus.Logger, output, logDir, level string, append bool) error {
	formatter := &logrus.TextFormatter{DisableSorting: true}
	if output == "stderr" {
		l.Out = os.Stderr
	} else if output == "stdout" {
		l.Out = os.Stdout
	} else if output == "discard" {
		l.Out = ioutil.Discard
	} else {
		formatter.DisableColors = true
		if out, err := touchLogOutput(output, logDir, append); err != nil {
			return err
		} else {
			util.AddHook(func() { out.Close() })
			l.Out = out
		}
	}
	l.Formatter = formatter
	if lvl, err := logrus.ParseLevel(level); err == nil {
		l.Level = lvl
	}
	return nil
}

func SetupLogger(output, logDir, level string, append bool) (err error) {
	once.Do(func() {
		err = setupLogger(paxosLogger, output, logDir, level, append)
	})
	return
}

func GetLogger(m string) Logger {
	return paxosLogger.WithField("module", m)
}

func touchLogOutput(output, logDir string, append bool) (*os.File, error) {
	if output == "" {
		output = "paxos.log"
	}
	if logDir == "" {
		logDir = "."
	} else {
		info, err := os.Stat(logDir)
		if err == nil {
			if !info.IsDir() {
				return nil, fmt.Errorf("logger: %s is not dir", logDir)
			}
		} else if os.IsNotExist(err) {
			if err = os.MkdirAll(logDir, 0755); err != nil {
				return nil, fmt.Errorf("logger: make dir %s error: %v", logDir, err)
			}
		}
	}
	var f *os.File
	file := path.Join(logDir, output)
	info, err := os.Stat(file)
	if err == nil {
		if info.IsDir() {
			return nil, fmt.Errorf("logger: output %s is dir", output)
		} else {
			var flag int
			if append {
				flag = os.O_RDWR | os.O_APPEND
			} else {
				flag = os.O_RDWR | os.O_TRUNC
			}
			f, err = os.OpenFile(file, flag, 0)
			if err != nil {
				err = fmt.Errorf("logger: open output %s error: %v", file, err)
			}
		}
	} else if os.IsNotExist(err) {
		f, err = os.Create(file)
		if err != nil {
			err = fmt.Errorf("logger: create output %s error: %v", file, err)
		}
	}
	return f, err
}

func setupDefaultLogger(l *defaultLogger, output, logDir, prefix, level string) (Logger, error) {
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
			util.AddHook(func() { out.Close() })
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
