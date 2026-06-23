package simlog

import "log"

type Level int

const (
	Quiet Level = -1
	Info  Level = 0
	Debug Level = 1
)

func Enabled(current Level, needed Level) bool {
	return current >= needed
}

func Logf(logger *log.Logger, current Level, needed Level, component string, format string, args ...any) {
	if logger == nil || !Enabled(current, needed) {
		return
	}

	logger.Printf("[%s] [%s] "+format, append([]any{needed.String(), component}, args...)...)
}

func (l Level) String() string {
	switch l {
	case Quiet:
		return "QUIET"
	case Info:
		return "INFO"
	case Debug:
		return "DEBUG"
	default:
		return "INFO"
	}
}
