## remove Dir to Path

```go
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
```

## Strategie

1. list l'ensemble des workspaces avec un list.go
2. remove all `.terraform` directory
3. retourne la list des workspace actif
4. run cmd `string[]{"init", "-upgrade"}` uniquement sur la list de workspaces actif

-------------------------------------

Si tu veux retrouver le fichier `terraform.tfstate` dans un répertoire, tu as plusieurs possibilités selon ton besoin.

### 1. Chercher uniquement à la racine du répertoire

Si le fichier est toujours directement dans le dossier :

```text id="m2h7cp"
/project/
├── terraform.tfstate
└── .terraform/
```

Tu peux simplement faire :

```go id="aqgxy0"
func FindTerraformState(root string) (string, bool, error) {
	path := filepath.Join(root, "terraform.tfstate")

	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", false, nil
		}
		return "", false, err
	}

	if info.IsDir() {
		return "", false, nil
	}

	return path, true, nil
}
```

Utilisation :

```go id="w63s2e"
path, found, err := FindTerraformState("/my/project")
if err != nil {
	return err
}

if found {
	fmt.Println(path)
}
```

---

### 2. Recherche récursive

Si le fichier peut être n'importe où :

```text id="6c4yq9"
/projects/
├── app1/
│   └── terraform.tfstate
├── app2/
│   └── envs/
│       └── prod/
│           └── terraform.tfstate
```

Utilise `filepath.WalkDir` :

```go id="gwdh32"
func FindTerraformState(root string) ([]string, error) {
	var states []string

	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() && d.Name() == "terraform.tfstate" {
			states = append(states, path)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return states, nil
}
```

Exemple :

```go id="ok96vt"
states, err := FindTerraformState("/projects")
if err != nil {
	return err
}

for _, state := range states {
	fmt.Println(state)
}
```

---

### 3. Version optimisée pour très gros volumes

Si tu parcours plus de 100 000 répertoires, éviter :

```go id="s3cw44"
var states []string
```

et préférer un callback :

```go id="4qq7px"
func FindTerraformState(root string, fn func(string) error) error {
	return filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() && d.Name() == "terraform.tfstate" {
			return fn(path)
		}

		return nil
	})
}
```

Utilisation :

```go id="4vh2ny"
err := FindTerraformState("/projects", func(path string) error {
	fmt.Println(path)

	// Lire, déplacer ou supprimer ici
	return nil
})
```

Cela garde une consommation mémoire constante.

---

Pour Terraform, le cas le plus fréquent est :

```text id="clo9of"
/my-project/
├── terraform.tfstate
├── terraform.tfstate.backup
└── .terraform/
```

Donc la version simple avec :

```go id="i1j17e"
filepath.Join(projectPath, "terraform.tfstate")
```

est souvent suffisante et beaucoup plus rapide qu'un `WalkDir`.
