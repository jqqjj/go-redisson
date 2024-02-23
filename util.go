package redisson

import (
	"runtime"
	"strconv"
	"strings"
)

// GoroutineID
// @Description: get the id of current goroutine
// @return int64
func GoroutineID() int64 {
	buf := make([]byte, 35)
	runtime.Stack(buf, false)
	s := string(buf)
	parseInt, _ := strconv.ParseInt(strings.TrimSpace(s[10:strings.IndexByte(s, '[')]), 10, 64)
	return parseInt
}

func ChannelName(name string) string {
	return "_redisson_channel_" + "{" + name + "}"
}

func LockName(name string) string {
	return "_redisson_lock_" + "{" + name + "}"
}
