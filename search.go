package main

import (
	"os"
	"path/filepath"
)

func findDirectoryInPath(name string) (string, bool) {
	pathEnv := os.Getenv("PATH")

	for _, dir := range filepath.SplitList(pathEnv) {
		candidate := filepath.Join(dir, name)

		info, err := os.Stat(candidate)
		if err != nil {
			continue
		}

		if info.IsDir() {
			return candidate, true
		}
	}

	return "", false
}
