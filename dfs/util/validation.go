package util

import (
	"bytes"
	"syscall"
)

func VerifyChecksum(checksum1 []byte, checksum2 []byte) bool {
	return bytes.Equal(checksum1, checksum2)
}

// in bytes
func GetDiskSpace() (uint64, error) {
	fs := syscall.Statfs_t{}
	err := syscall.Statfs(".", &fs)
	if err != nil {
		return 0, err
	}
	freeSpace := fs.Bfree * uint64(fs.Bsize)

	return freeSpace, nil
}

// check available space on disk
func CheckSpace(size int64) (bool, error) {
	freeSpace, err := GetDiskSpace()
	if err != nil {
		return false, err
	}

	return freeSpace-uint64(size) > 0, nil
}
