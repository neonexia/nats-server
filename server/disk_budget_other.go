//go:build !linux && !darwin

package server

import (
	"errors"
	"os"
)

func diskBudgetFree(string) (uint64, uint64, error) {
	return 0, 0, errors.New("disk_budget filesystem measurement is supported only on Linux and Darwin")
}
func diskBudgetAllocated(info os.FileInfo) uint64 { return diskBudgetRound(uint64(info.Size())) }
