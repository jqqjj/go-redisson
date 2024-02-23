package redisson

import (
	"runtime"
	"strconv"
	"strings"
)

// goroutineID
// @Description: get the id of current goroutine
// @return int64
func goroutineID() int64 {
	buf := make([]byte, 35)
	runtime.Stack(buf, false)
	s := string(buf)
	parseInt, _ := strconv.ParseInt(strings.TrimSpace(s[10:strings.IndexByte(s, '[')]), 10, 64)
	return parseInt
}

func channelName(name string) string {
	return "_redisson_channel_" + "{" + name + "}"
}

func lockName(name string) string {
	return "_redisson_lock_" + "{" + name + "}"
}
