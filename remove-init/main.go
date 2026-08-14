// terraform-cleaner : supprime les répertoires .terraform puis lance
// "terraform init -upgrade" dans chaque répertoire contenant un terraform.tfstate.
//
// Conçu pour parcourir 100 000+ chemins sans saturer la mémoire :
//   - filepath.WalkDir est entièrement streamé (aucune liste chargée en RAM)
//   - les répertoires de travail sont lus ligne à ligne avec bufio.Scanner
//   - les commandes terraform tournent dans un pool de workers bornés

package main

import (
	"bufio"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

const (
	terraformDirName = ".terraform"
	tfStateFileName  = "terraform.tfstate"
)

// ── flags ─────────────────────────────────────────────────────────────────────

var (
	flagRoot    = flag.String("root", "/workspaces", "Racine pour la suppression des .terraform")
	flagDirsArg = flag.String("dirs", "", "Chemin vers un fichier texte dont chaque ligne est un répertoire à scanner pour les tfstate (optionnel, défaut = root)")
	flagWorkers = flag.Int("workers", 6, "Nombre max de 'terraform init' lancés en parallèle")
	flagDryRun  = flag.Bool("dry-run", false, "Affiche les actions sans les exécuter")
	flagVerbose = flag.Bool("verbose", false, "Affiche la sortie complète de chaque 'terraform init'")
)

func main() {
	flag.Parse()
	log.SetFlags(log.Ltime | log.Lmsgprefix)

	// ── Phase 1 : suppression des répertoires .terraform ──────────────────────
	fmt.Println()
	log.SetPrefix("[phase-1] ")
	log.Printf("Suppression des répertoires '%s' sous : %s", terraformDirName, *flagRoot)

	var removed, skipped int64
	if err := removeTerraformDirs(*flagRoot, *flagDryRun, &removed, &skipped); err != nil {
		log.Fatalf("Erreur phase 1 : %v", err)
	}
	log.Printf("Terminé — supprimés : %d | ignorés (erreurs) : %d", removed, skipped)

	// ── Phase 2 : terraform init -upgrade ─────────────────────────────────────
	fmt.Println()
	log.SetPrefix("[phase-2] ")
	log.Println("Recherche des tfstate et lancement de 'terraform init -upgrade'…")

	dirs, cleanup, err := resolveDirs(*flagDirsArg, *flagRoot)
	if err != nil {
		log.Fatalf("Impossible de résoudre la liste de répertoires : %v", err)
	}
	defer cleanup()

	var found, success, failure int64
	if err := runTerraformInit(dirs, *flagWorkers, *flagDryRun, *flagVerbose,
		&found, &success, &failure); err != nil {
		log.Fatalf("Erreur phase 2 : %v", err)
	}
	log.Printf("Terminé — tfstate trouvés : %d | succès : %d | échecs : %d",
		found, success, failure)
	fmt.Println()
}

// ── Phase 1 ───────────────────────────────────────────────────────────────────

// removeTerraformDirs parcourt root en streaming et supprime chaque répertoire
// nommé ".terraform". Quand il en trouve un, il le supprime et saute sa descente
// (fs.SkipDir) : pas besoin d'inspecter le contenu.
func removeTerraformDirs(root string, dryRun bool, removed, skipped *int64) error {
	return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			// Problème d'accès : on log et on continue, pas d'arrêt brutal.
			log.Printf("Accès impossible : %s — %v", path, err)
			atomic.AddInt64(skipped, 1)
			return nil
		}

		if !d.IsDir() || d.Name() != terraformDirName {
			return nil
		}

		if dryRun {
			log.Printf("[dry-run] Supprimerais : %s", path)
		} else {
			if err := os.RemoveAll(path); err != nil {
				log.Printf("Échec suppression %s : %v", path, err)
				atomic.AddInt64(skipped, 1)
			} else {
				log.Printf("Supprimé : %s", path)
				atomic.AddInt64(removed, 1)
			}
		}

		// Ne pas descendre dans .terraform, même en cas d'erreur.
		return fs.SkipDir
	})
}

// ── Phase 2 ───────────────────────────────────────────────────────────────────

// resolveDirs renvoie un canal (streamé) de chemins de répertoires à scanner.
// Si dirsFile est renseigné, les lignes du fichier sont lues une par une
// (bufio.Scanner) sans tout charger en mémoire. Sinon on utilise root.
// cleanup ferme le fichier si besoin.
func resolveDirs(dirsFile, root string) (<-chan string, func(), error) {
	ch := make(chan string, 64) // tampon léger pour découpler I/O et traitement

	if dirsFile == "" {
		// Pas de fichier : on n'a qu'un seul répertoire, la racine.
		go func() {
			defer close(ch)
			ch <- root
		}()
		return ch, func() {}, nil
	}

	f, err := os.Open(dirsFile)
	if err != nil {
		return nil, func() {}, fmt.Errorf("ouverture de %s : %w", dirsFile, err)
	}

	go func() {
		defer close(ch)
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			line := sc.Text()
			if line == "" {
				continue
			}
			ch <- line
		}
		if err := sc.Err(); err != nil {
			log.Printf("Erreur lecture %s : %v", dirsFile, err)
		}
	}()

	return ch, func() { f.Close() }, nil
}

// runTerraformInit consomme le canal de répertoires, marche chacun en streaming
// et dispatche chaque tfstate trouvé vers un pool de workers bornés.
func runTerraformInit(dirs <-chan string, maxWorkers int, dryRun, verbose bool,
	found, success, failure *int64) error {

	sem := make(chan struct{}, maxWorkers) // sémaphore = borne de parallélisme
	var wg sync.WaitGroup

	for dir := range dirs {
		dir := dir

		walkErr := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				log.Printf("Accès impossible : %s — %v", path, err)
				return nil // on continue la marche
			}

			// On ne s'intéresse qu'au fichier terraform.tfstate.
			if d.IsDir() || d.Name() != tfStateFileName {
				return nil
			}

			stateDir := filepath.Dir(path)
			atomic.AddInt64(found, 1)

			// Acquisition du sémaphore AVANT le goroutine pour éviter de
			// créer des milliers de goroutines dormantes.
			sem <- struct{}{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				execTerraformInit(stateDir, dryRun, verbose, success, failure)
			}()

			return nil
		})

		if walkErr != nil {
			log.Printf("Erreur walk %s : %v", dir, walkErr)
		}
	}

	wg.Wait()
	return nil
}

// execTerraformInit lance "terraform init -upgrade" dans le répertoire donné.
func execTerraformInit(dir string, dryRun, verbose bool, success, failure *int64) {
	if dryRun {
		log.Printf("[dry-run] terraform init -upgrade  dans : %s", dir)
		atomic.AddInt64(success, 1)
		return
	}

	start := time.Now()
	cmd := exec.Command("terraform", "init", "-upgrade")
	cmd.Dir = dir

	out, err := cmd.CombinedOutput()
	elapsed := time.Since(start).Round(time.Millisecond)

	if err != nil {
		atomic.AddInt64(failure, 1)
		log.Printf("ÉCHEC (%s) dans %s : %v", elapsed, dir, err)
		// Toujours afficher la sortie en cas d'erreur pour faciliter le debug.
		if len(out) > 0 {
			fmt.Fprintf(os.Stderr, "--- sortie [%s] ---\n%s\n---\n", dir, out)
		}
		return
	}

	atomic.AddInt64(success, 1)
	log.Printf("OK (%s) : %s", elapsed, dir)
	if verbose && len(out) > 0 {
		fmt.Printf("--- sortie [%s] ---\n%s\n---\n", dir, out)
	}
}
