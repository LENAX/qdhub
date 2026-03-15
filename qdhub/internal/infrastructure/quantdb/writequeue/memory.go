package writequeue

import (
	"runtime"

	"github.com/sirupsen/logrus"
)

// MemStatus indicates the current memory pressure level.
type MemStatus int

const (
	MemStatusNormal MemStatus = iota
	MemStatusHigh
	MemStatusCritical
)

// CheckMemoryStatus checks the current memory usage against configured limits.
func CheckMemoryStatus(enabled bool, highMB, criticalMB int) MemStatus {
	if !enabled {
		return MemStatusNormal
	}

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// HeapInuse is a good approximation of memory actually in use by Go.
	// Sys is the total memory obtained from the OS.
	// We'll use HeapInuse + roughly what's held but not released (HeapIdle - HeapReleased)
	// But simple HeapInuse or Alloc is a good start. Let's use Alloc.
	usedMB := int(m.Alloc / 1024 / 1024)

	if criticalMB > 0 && usedMB >= criticalMB {
		logrus.Warnf("[WriteQueue] Memory is CRITICAL: used=%dMB, limit=%dMB", usedMB, criticalMB)
		return MemStatusCritical
	}

	if highMB > 0 && usedMB >= highMB {
		logrus.Warnf("[WriteQueue] Memory is HIGH: used=%dMB, limit=%dMB", usedMB, highMB)
		return MemStatusHigh
	}

	return MemStatusNormal
}
