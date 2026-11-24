//go:build !unix

package walframe

import "time"

func cpuTime() time.Duration { return 0 }
