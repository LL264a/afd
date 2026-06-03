package task

import "fmt"

func CheckDiskSpace(path string, requiredSize int64) error {
	available, err := GetAvailableSpace(path)
	if err != nil {
		return err
	}

	if available < requiredSize {
		return fmt.Errorf("insufficient disk space: required %d bytes, available %d bytes", requiredSize, available)
	}

	return nil
}
