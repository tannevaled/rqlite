//go:build unix

package walframe

import (
	"syscall"
	"time"
)

// cpuTime is this process's user plus system CPU time.
func cpuTime() time.Duration {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}
