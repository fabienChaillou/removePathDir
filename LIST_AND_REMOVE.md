Oui. Pour **100 000+ paths**, je te conseille de ne pas construire une liste en mémoire. Le plus propre est :

* lire les paths en streaming ;
* avoir un petit `channel` bufferisé ;
* utiliser un pool de workers ;
* pour chaque path, tester `<path>/.terraform` ;
* supprimer avec `os.RemoveAll()` ;
* ne conserver aucun des 100 000 paths en mémoire.

### Fonction complète

```go
package terraform

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type CleanupStats struct {
	Processed int
	Found     int
	Deleted   int
	NotFound  int
	Errors    int
}

func CleanupTerraformDirectories(
	paths <-chan string,
	workers int,
) CleanupStats {
	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		stats CleanupStats
	)

	if workers <= 0 {
		workers = 1
	}

	worker := func() {
		defer wg.Done()

		for basePath := range paths {
			mu.Lock()
			stats.Processed++
			mu.Unlock()

			terraformPath := filepath.Join(basePath, ".terraform")

			info, err := os.Lstat(terraformPath)
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					mu.Lock()
					stats.NotFound++
					mu.Unlock()
					continue
				}

				mu.Lock()
				stats.Errors++
				mu.Unlock()

				continue
			}

			if !info.IsDir() {
				mu.Lock()
				stats.Errors++
				mu.Unlock()

				continue
			}

			mu.Lock()
			stats.Found++
			mu.Unlock()

			if err := os.RemoveAll(terraformPath); err != nil {
				mu.Lock()
				stats.Errors++
				mu.Unlock()

				continue
			}

			mu.Lock()
			stats.Deleted++
			mu.Unlock()
		}
	}

	wg.Add(workers)

	for i := 0; i < workers; i++ {
		go worker()
	}

	wg.Wait()

	return stats
}
```

Mais pour ton cas, je préfère encore une API qui **prend directement le fichier contenant les 100 000 paths**, puisqu'on parlait précédemment d'une liste de paths.

### Version que j'utiliserais réellement

```go
package terraform

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type CleanupStats struct {
	Processed int
	Found     int
	Deleted   int
	NotFound  int
	Errors    int
}

func CleanupTerraformDirectoriesFromFile(
	filePath string,
	workers int,
) (CleanupStats, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return CleanupStats{}, fmt.Errorf("open paths file: %w", err)
	}
	defer file.Close()

	if workers <= 0 {
		workers = 1
	}

	jobs := make(chan string, workers*2)

	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		stats CleanupStats
	)

	// Workers
	wg.Add(workers)

	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()

			for basePath := range jobs {
				mu.Lock()
				stats.Processed++
				mu.Unlock()

				terraformPath := filepath.Join(basePath, ".terraform")

				info, err := os.Lstat(terraformPath)

				if err != nil {
					if errors.Is(err, os.ErrNotExist) {
						mu.Lock()
						stats.NotFound++
						mu.Unlock()
						continue
					}

					mu.Lock()
					stats.Errors++
					mu.Unlock()
					continue
				}

				if !info.IsDir() {
					mu.Lock()
					stats.Errors++
					mu.Unlock()
					continue
				}

				mu.Lock()
				stats.Found++
				mu.Unlock()

				if err := os.RemoveAll(terraformPath); err != nil {
					mu.Lock()
					stats.Errors++
					mu.Unlock()
					continue
				}

				mu.Lock()
				stats.Deleted++
				mu.Unlock()
			}
		}()
	}

	// Lecture streaming
	scanner := bufio.NewScanner(file)

	// Permet d'accepter des paths potentiellement très longs.
	scanner.Buffer(
		make([]byte, 64*1024),
		1024*1024,
	)

	for scanner.Scan() {
		path := scanner.Text()

		if path == "" {
			continue
		}

		jobs <- path
	}

	close(jobs)

	wg.Wait()

	if err := scanner.Err(); err != nil {
		return stats, fmt.Errorf("read paths file: %w", err)
	}

	return stats, nil
}
```

Utilisation :

```go
stats, err := CleanupTerraformDirectoriesFromFile(
	"paths.txt",
	10,
)

if err != nil {
	log.Fatal(err)
}

fmt.Printf(
	"processed=%d found=%d deleted=%d not_found=%d errors=%d\n",
	stats.Processed,
	stats.Found,
	stats.Deleted,
	stats.NotFound,
	stats.Errors,
)
```

Avec :

```text
100 000 paths
        │
        ▼
    Scanner
        │
        ▼
   channel (20)
        │
   ┌────┼────┐
   ▼    ▼    ▼
  W1   W2   ... W10
   │    │
   ▼    ▼
path/.terraform
        │
        ▼
    RemoveAll()
```

### Pourquoi cette solution est adaptée

Pour **100 000, 1 million ou 10 millions de paths**, la mémoire reste pratiquement constante :

* pas de `[]string` de 100 000 éléments ;
* `Scanner` lit ligne par ligne ;
* seulement `workers × 2` paths sont en attente ;
* `RemoveAll()` travaille directement sur le filesystem ;
* seulement quelques compteurs sont conservés.

Je commencerais avec **8 à 16 workers**. Pour un filesystem local ou un volume réseau, augmenter à 100 workers n'est pas forcément plus rapide : tu risques surtout de saturer les I/O.

**Petite amélioration que je ferais ensuite :** remplacer les `sync.Mutex` autour de chaque compteur par des `atomic.Int64`. Avec 100 000+ opérations, ça simplifie le code et réduit le coût de synchronisation.
