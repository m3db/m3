package main

// This .go file is used to test the lockfile package (m3/src/x/lockfile)

import (
	"fmt"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/m3db/m3/src/x/lockfile"
)

func exitWithUsage() {
	fmt.Printf(
		"Usage: %[1]s <LOCK_FILE_PATH> <NUM_SECONDS_TO_SLEEP> <SHOULD_CLEANUP_LOCK>\nExample: %[1]s /var/run/lockfile 1 1\n",
		path.Base(os.Args[0]))
	os.Exit(1)
}

func main() {
	if len(os.Args) != 4 {
		exitWithUsage()
	}

	path, sleepStr, rmLock := os.Args[1], os.Args[2], os.Args[3]
	sleep, err := strconv.Atoi(sleepStr)
	if err != nil {
		exitWithUsage()
	}

	lock, err := lockfile.Acquire(path)
	if err != nil {
		os.Exit(1)
	}

	if sleep > 0 {
		time.Sleep(time.Duration(sleep) * time.Second)
	}

	if rmLock != "0" {
		lock.Release()
	}
}
