package hal

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

type cpuTicks struct {
	idle  uint64
	total uint64
}

// Sampler reads real host metrics from Linux pseudo-filesystems.
type Sampler struct {
	mu       sync.Mutex
	prevCPU  cpuTicks
	hasPrev  bool
	tempPath string
}

// NewSampler initializes a Linux hardware/OS metrics sampler.
func NewSampler() *Sampler {
	return &Sampler{
		tempPath: findThermalZonePath(),
	}
}

// Temperature does not have same path for different devices, it has to be found
func findThermalZonePath() string {
	matches, err := filepath.Glob("/sys/class/thermal/thermal_zone*/temp")
	if err != nil || len(matches) == 0 {
		return "/sys/class/thermal/thermal_zone0/temp"
	}
	return matches[0]
}

// CPUUsagePercent computes CPU utilization percentage between consecutive calls.
func (s *Sampler) CPUUsagePercent() (float32, error) {
	data, err := os.ReadFile("/proc/stat")
	if err != nil {
		return 0, err
	}

	var curr cpuTicks
	found := false

	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "cpu ") {
			fields := strings.Fields(line)
			if len(fields) < 5 {
				return 0, fmt.Errorf("unexpected /proc/stat cpu line")
			}

			var vals []uint64
			for _, f := range fields[1:] {
				v, parseErr := strconv.ParseUint(f, 10, 64)
				if parseErr != nil {
					return 0, parseErr
				}
				vals = append(vals, v)
			}

			// idle = idle (index 3) + iowait (index 4 when present)
			idle := vals[3]
			if len(vals) > 4 {
				idle += vals[4]
			}

			var total uint64
			for _, v := range vals {
				total += v
			}

			curr = cpuTicks{idle: idle, total: total}
			found = true
			break
		}
	}

	if !found {
		return 0, fmt.Errorf("cpu line not found in /proc/stat")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.hasPrev {
		s.prevCPU = curr
		s.hasPrev = true
		return 0, nil
	}

	deltaTotal := curr.total - s.prevCPU.total
	deltaIdle := curr.idle - s.prevCPU.idle
	s.prevCPU = curr

	if deltaTotal == 0 || deltaIdle >= deltaTotal {
		return 0, nil
	}

	usage := (float64(deltaTotal-deltaIdle) / float64(deltaTotal)) * 100.0
	return float32(usage), nil
}

// MemoryFreeBytes reads MemAvailable from /proc/meminfo.
func (s *Sampler) MemoryFreeBytes() (uint64, error) {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, err
	}

	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "MemAvailable:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				kb, parseErr := strconv.ParseUint(fields[1], 10, 64)
				if parseErr != nil {
					return 0, parseErr
				}
				return kb * 1024, nil
			}
		}
	}

	return 0, fmt.Errorf("MemAvailable metric not found in /proc/meminfo")
}

// TemperatureCelsius reads thermal zone millidegrees and converts to Celsius.
func (s *Sampler) TemperatureCelsius() (float32, error) {
	s.mu.Lock()
	path := s.tempPath
	s.mu.Unlock()

	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}

	tempStr := strings.TrimSpace(string(data))
	milliDegrees, err := strconv.Atoi(tempStr)
	if err != nil {
		return 0, err
	}

	return float32(milliDegrees) / 1000.0, nil
}

func (s *Sampler) UptimeSeconds() (uint64, error) {
	data, err := os.ReadFile("/proc/uptime")
	if err != nil {
		return 0, err
	}

	fields := strings.Fields(string(data))
	if len(fields) == 0 {
		return 0, fmt.Errorf("empty /proc/uptime")
	}

	seconds, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return 0, err
	}
	return uint64(seconds), nil
}
