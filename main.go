package main

import (
	"fmt"
	//"mxshs/brc/three_thread_groups"
    "mxshs/brc/naive"
	"runtime/debug"
	"time"
)

func main() {
    debug.SetGCPercent(1000)
    start := time.Now()

    naive.Parse("measurements.txt", 64 * 1_000_000, 12, 3)

    fmt.Printf("brc took %s", time.Since(start).String())
}
