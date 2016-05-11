package fs

import (
	"os"
)

type fileOpener func(filePath string) (*os.File, error)

func openFilesWithFilePathPrefix(opener fileOpener, fds map[string]**os.File) error {
	for filePath, fdPtr := range fds {
		fd, err := opener(filePath)
		if err != nil {
			return err
		}
		*fdPtr = fd
	}
	return nil
}

func closeFiles(fds ...*os.File) error {
	for _, fd := range fds {
		if err := fd.Close(); err != nil {
			return err
		}
	}
	return nil
}

func filenameFromPrefix(prefix, name string) string {
	// Currently just take prefix as absolute prefix, use no join character
	return prefix + name
}
