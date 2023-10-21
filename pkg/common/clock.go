package common

import "time"

// for time mock
type Clock interface {
	Now() time.Time
}

type DefalutClock struct{}

func NewDefaultClock() Clock {
	return &DefalutClock{}
}

func (c *DefalutClock) Now() time.Time {
	return time.Now()
}
