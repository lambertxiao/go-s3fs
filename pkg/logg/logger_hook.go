//go:build !windows
// +build !windows

package logg

import (
	"fmt"
	"log/syslog"
	"os"
	"path"
	"path/filepath"
	"time"

	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	"github.com/rifflock/lfshook"
	"github.com/sirupsen/logrus"
	lsys "github.com/sirupsen/logrus/hooks/syslog"
)

const (
	logSuffix = "%Y%m%d-%H.log"
)

func InitLogHook(logDir string, logMaxAge, logRotationTime time.Duration) {
	var err error
	if logDir != "" {
		err = os.MkdirAll(filepath.Dir(logDir), os.ModePerm)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create log dir")
			return
		}

		defaultLogHook, err = newRotatelogHook(
			path.Join(logDir, "go-s3fs-"+logSuffix), "go-s3fs", logMaxAge, logRotationTime,
		)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create default log hook")
			return
		}

		fuseLogHook, err = newRotatelogHook(
			path.Join(logDir, "fuse-"+logSuffix), "fuse", logMaxAge, logRotationTime,
		)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create fuse log hook")
			return
		}
	} else {
		syslogHook, err = lsys.NewSyslogHook("", "", syslog.LOG_DEBUG, "")
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create syslog hook")
			return
		}
	}
}

func newRotatelogHook(logPath, module string, maxAage, rotationTime time.Duration) (logrus.Hook, error) {
	writer, err := rotatelogs.New(
		logPath,
		rotatelogs.WithMaxAge(maxAage),
		rotatelogs.WithRotationTime(rotationTime),
	)
	if err != nil {
		return nil, err
	}

	writeMap := lfshook.WriterMap{
		logrus.InfoLevel:  writer,
		logrus.FatalLevel: writer,
		logrus.DebugLevel: writer,
		logrus.WarnLevel:  writer,
		logrus.ErrorLevel: writer,
		logrus.PanicLevel: writer,
	}

	formatter := &CommonLogFormatter{
		pid: os.Getpid(),
	}
	return lfshook.NewHook(writeMap, formatter), nil
}
