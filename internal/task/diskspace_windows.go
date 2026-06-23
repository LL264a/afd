package task

import (
	"fmt"
	"sync"
	"syscall"
	"unsafe"
)

var (
	kernel32Once sync.Once
	kernel32DLL  *syscall.DLL
	diskFreeProc *syscall.Proc
	diskFreeErr  error
)

func initDiskFreeSpace() {
	kernel32Once.Do(func() {
		kernel32DLL, diskFreeErr = syscall.LoadDLL("kernel32.dll")
		if diskFreeErr != nil {
			return
		}
		diskFreeProc, diskFreeErr = kernel32DLL.FindProc("GetDiskFreeSpaceExW")
	})
}

func GetAvailableSpace(path string) (int64, error) {
	initDiskFreeSpace()
	if diskFreeErr != nil {
		return 0, diskFreeErr
	}

	pathPtr, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}

	var freeBytesAvailable, totalNumberOfBytes, totalNumberOfFreeBytes int64

	ret, _, lastErr := diskFreeProc.Call(
		uintptr(unsafe.Pointer(pathPtr)),
		uintptr(unsafe.Pointer(&freeBytesAvailable)),
		uintptr(unsafe.Pointer(&totalNumberOfBytes)),
		uintptr(unsafe.Pointer(&totalNumberOfFreeBytes)),
	)

	if ret == 0 {
		return 0, fmt.Errorf("GetDiskFreeSpaceExW failed: %w", lastErr)
	}

	return freeBytesAvailable, nil
}
