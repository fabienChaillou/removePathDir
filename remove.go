package main

import (
	"fmt"
	"os"
	"path/filepath"
)

func removeDirectoryFromPath(name string) error {
	for _, dir := range filepath.SplitList(os.Getenv("PATH")) {
		candidate := filepath.Join(dir, name)

		info, err := os.Stat(candidate)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}

		if !info.IsDir() {
			continue
		}

		if err := os.RemoveAll(candidate); err != nil {
			return fmt.Errorf("suppression de %s : %w", candidate, err)
		}

		fmt.Printf("Directory supprimé : %s\n", candidate)
		return nil
	}

	return nil
}
