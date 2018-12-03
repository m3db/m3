package lockfile

import (
	"os"
	"syscall"
)

type Lockfile struct {
	file os.File
}

// Create creates the given file path if it doesn't exist and
// obtains an exclusive lock on it. An error is returned if the lock
// has been obtained by another process.
func Create(path string) (*Lockfile, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	ft := &syscall.Flock_t{
		Pid:  int32(os.Getpid()),
		Type: syscall.F_WRLCK,
	}

	if err = syscall.FcntlFlock(file.Fd(), syscall.F_SETLK, ft); err != nil {
		return nil, err
	}

	lf := Lockfile{*file}

	return &lf, nil
}

// Remove releases the lock on the file and removes the file.
func (lf Lockfile) Remove() error {
	ft := &syscall.Flock_t{
		Pid:  int32(os.Getpid()),
		Type: syscall.F_UNLCK,
	}

	if err := syscall.FcntlFlock(lf.file.Fd(), syscall.F_SETLK, ft); err != nil {
		return err
	}

	if err := os.Remove(lf.file.Name()); err != nil {
		return err
	}

	if err := lf.file.Close(); err != nil {
		return err
	}

	return nil
}
