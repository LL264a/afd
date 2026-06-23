//go:build netbsd && !js

package task

func GetAvailableSpace(path string) (int64, error) {
	return 1<<63 - 1, nil // report max available on unsupported platforms
}
