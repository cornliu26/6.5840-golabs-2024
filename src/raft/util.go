package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

var debugStart time.Time
var debugVerbosity int

const isDebug = false

const recordNum = 5

func DPrintf(format string, a ...interface{}) {
	if isDebug {
		log.Printf(format, a...)
	}
}

func Debug(rf *Raft, format string, a ...interface{}) {
	if (isDebug || debugVerbosity > 0) && rf.me < recordNum {
		time := time.Since(debugStart).Milliseconds()
		prefix := fmt.Sprintf("%06d T%03d R%02d ", time, rf.currentTerm, rf.me)
		format = prefix + strings.Repeat("          ", rf.me) + format
		log.Printf(format, a...)
	}
}

func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
