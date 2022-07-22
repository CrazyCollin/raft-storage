package log

import (
	"fmt"
	"github.com/phuslu/log"
	"io"
	"strings"
)

var MainLogger *log.Logger

func init() {
	MainLogger = &log.Logger{
		Level: log.DebugLevel,
		Writer: &log.ConsoleWriter{
			Formatter: func(w io.Writer, a *log.FormatterArgs) (int, error) {
				return fmt.Fprintf(w, "%c%s %s %s] %s\n%s", strings.ToUpper(a.Level)[0],
					a.Time, a.Goid, a.Caller, a.Message, a.Stack)
			},
		},
	}
}
