//go:build linux || darwin

package server

import (
	"fmt"
	"os"
	"syscall"
)

func diskBudgetFree(root string) (uint64, uint64, error) {
	var fs syscall.Statfs_t
	if err := syscall.Statfs(root, &fs); err != nil {
		return 0, 0, err
	}
	if fs.Files > 0 && fs.Ffree < 128 {
		return uint64(fs.Bavail) * uint64(fs.Bsize), 0, fmt.Errorf("filesystem inode reserve is below 128")
	}
	return uint64(fs.Bavail) * uint64(fs.Bsize), uint64(fs.Ffree), nil
}
func diskBudgetAllocated(info os.FileInfo) uint64 {
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		return max(uint64(stat.Blocks)*512, diskBudgetRound(uint64(info.Size())))
	}
	return diskBudgetRound(uint64(info.Size()))
}
