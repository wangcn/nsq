package defer_queue

import "github.com/nsqio/nsq/internal/lg"

type Logger struct {
	lg.Logger
	BaseLevel lg.LogLevel
}
