package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
)

const (
	workers   = 10
	bufferSize = 20
)

func cleanTerraformDirs(listFile string) error {
	file, err := os.Open(listFile)
	if err != nil {
		return fmt.Errorf("open paths file: %w", err)
	}
	defer file.Close()

	jobs := make(chan string, bufferSize)

	var wg sync.WaitGroup

	// Workers
	for i := 0; i < workers; i++ {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()

			for basePath := range jobs {
				if err := cleanTerraformDir(basePath); err != nil {
					log.Printf(
						"[worker %d] %s: %v",
						id,
						basePath,
						err,
					)
				}
			}
		}(i)
	}

	// Lecture streaming des paths
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		basePath := scanner.Text()

		if basePath == "" {
			continue
		}

		jobs <- basePath
	}

	close(jobs)
	wg.Wait()

	return scanner.Err()
}

func cleanTerraformDir(basePath string) error {
	terraformDir := filepath.Join(basePath, ".terraform")

	info, err := os.Lstat(terraformDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}

		return err
	}

	if !info.IsDir() {
		return fmt.Errorf("%s existe mais n'est pas un directory", terraformDir)
	}

	return os.RemoveAll(terraformDir)
}

func main() {
	if err := cleanTerraformDirs("paths.txt"); err != nil {
		log.Fatal(err)
	}
}
