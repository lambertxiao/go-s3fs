package logg

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/sirupsen/logrus"
)

// log instance
var Dlog, Dfuseerrlog, Dfuselog *LogHandle

func InitLogger() {
	Dlog = getLogger()
	Dfuseerrlog = getLogger()
	Dfuselog = getFuseLogger()
}

var (
	defaultLogHook, fuseLogHook, syslogHook logrus.Hook
)

type logger struct {
	level logrus.Level
}

type LogHandle struct {
	logrus.Logger
}

var dlogger = logger{
	level: logrus.InfoLevel,
}

func getLogger() *LogHandle {
	h := newHandle()
	if defaultLogHook != nil {
		h.Hooks.Add(defaultLogHook)
	} else if syslogHook != nil {
		h.Hooks.Add(syslogHook)
	}

	return h
}

func getFuseLogger() *LogHandle {
	h := newHandle()
	if fuseLogHook != nil {
		h.Hooks.Add(fuseLogHook)
	} else if syslogHook != nil {
		h.Hooks.Add(syslogHook)
	}
	return h
}

func newHandle() *LogHandle {
	l := &LogHandle{}
	l.Out = os.Stdout
	l.Hooks = make(logrus.LevelHooks)
	l.Formatter = &CommonLogFormatter{
		pid: os.Getpid(),
	}
	l.Level = dlogger.level
	l.SetReportCaller(true)

	return l
}

func SetLevel(level logrus.Level) {
	dlogger.level = level
}

type CommonLogFormatter struct {
	pid int
}

func (hook *CommonLogFormatter) Format(e *logrus.Entry) ([]byte, error) {
	var caller string
	if e.HasCaller() {
		callerPath := path.Join(path.Base(path.Dir(e.Caller.File)), path.Base(e.Caller.File))
		caller = fmt.Sprintf("%s:%d", callerPath, e.Caller.Line)
	}
	timestamp := e.Time.Format("2006-01-02 15:04:05.000000") + " "
	ret := new(bytes.Buffer)
	fmt.Fprintf(ret, "%v%d %v %v %s", timestamp, hook.pid, strings.ToUpper(e.Level.String()), e.Message, caller)

	if len(e.Data) != 0 {
		ret.WriteString(" " + fmt.Sprint(e.Data))
	}

	ret.WriteString("\n")
	return ret.Bytes(), nil
}
