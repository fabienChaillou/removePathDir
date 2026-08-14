// Package initializer implémente la Phase 2 du workflow Temporal :
//  1. Récupérer la liste d'UUIDs de workspaces via une API externe (simulée).
//  2. Pour chaque UUID, marcher /workspaces/{UUID} à la recherche de terraform.tfstate.
//  3. Lancer "terraform init -upgrade" dans chaque répertoire contenant un tfstate.
//
// Conception mémoire (100 000+ UUIDs) :
//   - L'API est consommée via un <-chan string : un UUID en mémoire à la fois.
//   - filepath.WalkDir est streamé : aucune liste n'est allouée.
//   - Le pool de workers est borné par un sémaphore canal : N goroutines max.
//   - Le context Temporal est propagé partout (annulation, heartbeat).
package initializer

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

const tfStateFile = "terraform.tfstate"

// ── API Fetcher ───────────────────────────────────────────────────────────────

// UUIDFetcher est la signature de toute fonction capable de streamer des UUIDs
// depuis une source externe. Le channel doit être fermé quand tous les UUIDs
// ont été émis (ou que le context est annulé).
//
// Temporal appellera cette fonction une fois ; le workflow consomme le canal
// au fil de l'eau sans jamais charger la liste complète en mémoire.
type UUIDFetcher func(ctx context.Context) (<-chan string, error)

// SimulatedAPIFetcher simule un appel paginé à une API REST externe.
// En production, remplacez cette implémentation par un vrai client HTTP
// qui pagine (cursor / offset) et envoie chaque UUID dans le channel.
//
// Comportement simulé : 5 pages de 20 UUIDs chacune, délai de 5 ms par page
// pour imiter la latence réseau.
func SimulatedAPIFetcher(pageSize int, totalPages int) UUIDFetcher {
	return func(ctx context.Context) (<-chan string, error) {
		ch := make(chan string, pageSize)

		go func() {
			defer close(ch)

			for page := 0; page < totalPages; page++ {
				// Simulation d'un appel HTTP paginé.
				time.Sleep(5 * time.Millisecond)

				for i := 0; i < pageSize; i++ {
					uuid := fmt.Sprintf("uuid-%04d-%04d", page, i)

					select {
					case ch <- uuid:
					case <-ctx.Done():
						return // annulation Temporal
					}
				}
			}
		}()

		return ch, nil
	}
}

// ── Command runner (injectable pour les tests) ────────────────────────────────

// CmdRunner est la signature de la fonction qui exécute une commande shell.
// En production c'est defaultCmdRunner ; dans les tests on injecte un mock.
type CmdRunner func(dir, name string, args ...string) ([]byte, error)

func defaultCmdRunner(dir, name string, args ...string) ([]byte, error) {
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	return cmd.CombinedOutput()
}

// ── Options ───────────────────────────────────────────────────────────────────

// Options configure le comportement de l'initialiseur.
type Options struct {
	// WorkspacesRoot est la racine des workspaces, ex. "/workspaces".
	// Chaque UUID sera résolu en WorkspacesRoot + "/" + UUID.
	WorkspacesRoot string

	// Workers est le nombre maximum de "terraform init" parallèles (défaut : 6).
	Workers int

	// DryRun affiche les actions sans les exécuter.
	DryRun bool

	// Verbose affiche la sortie complète de terraform même en cas de succès.
	Verbose bool

	// Logger (défaut : slog.Default()).
	Logger *slog.Logger

	// RunCommand permet d'injecter un exécuteur de commandes (tests uniquement).
	// Si nil, exec.Command est utilisé.
	RunCommand CmdRunner
}

func (o Options) workers() int {
	if o.Workers > 0 {
		return o.Workers
	}
	return 6
}

func (o Options) logger() *slog.Logger {
	if o.Logger != nil {
		return o.Logger
	}
	return slog.Default()
}

func (o Options) runner() CmdRunner {
	if o.RunCommand != nil {
		return o.RunCommand
	}
	return defaultCmdRunner
}

// ── Result ────────────────────────────────────────────────────────────────────

// Result résume l'exécution de la phase 2.
type Result struct {
	UUIDs   int64 // UUIDs consommés depuis l'API
	Found   int64 // fichiers tfstate trouvés
	Success int64 // "terraform init" réussis
	Failure int64 // "terraform init" en échec
}

func (r Result) String() string {
	return fmt.Sprintf("uuids=%d found=%d success=%d failure=%d",
		r.UUIDs, r.Found, r.Success, r.Failure)
}

// ── Point d'entrée principal ──────────────────────────────────────────────────

// RunTerraformInit est la fonction appelée par le workflow Temporal de phase 2.
//
//  1. fetch(ctx) ouvre le canal d'UUIDs (appel API paginé).
//  2. Pour chaque UUID, WorkspacesRoot/{UUID} est parcouru en streaming.
//  3. Chaque terraform.tfstate trouvé déclenche "terraform init -upgrade"
//     via le pool de workers borné.
func RunTerraformInit(ctx context.Context, fetch UUIDFetcher, opts Options) (Result, error) {
	log := opts.logger()
	run := opts.runner()

	uuids, err := fetch(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("fetch UUIDs: %w", err)
	}

	var res Result
	sem := make(chan struct{}, opts.workers()) // sémaphore = borne du pool
	var wg sync.WaitGroup

	for uuid := range uuids {
		// Annulation Temporal entre deux UUIDs.
		if ctx.Err() != nil {
			break
		}

		atomic.AddInt64(&res.UUIDs, 1)
		wsPath := filepath.Join(opts.WorkspacesRoot, uuid)

		walkErr := filepath.WalkDir(wsPath, func(path string, d fs.DirEntry, wErr error) error {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if wErr != nil {
				log.Warn("accès impossible", "path", path, "err", wErr)
				return nil
			}
			if d.IsDir() || d.Name() != tfStateFile {
				return nil
			}

			stateDir := filepath.Dir(path)
			atomic.AddInt64(&res.Found, 1)

			// Acquisition du sémaphore AVANT la goroutine pour ne pas créer
			// des dizaines de milliers de goroutines dormantes.
			sem <- struct{}{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				execInit(ctx, stateDir, run, opts, log, &res.Success, &res.Failure)
			}()

			return nil
		})

		if walkErr != nil && ctx.Err() == nil {
			log.Warn("walk error", "uuid", uuid, "path", wsPath, "err", walkErr)
		}
	}

	wg.Wait()

	if ctx.Err() != nil {
		return res, ctx.Err()
	}
	return res, nil
}

// execInit lance "terraform init -upgrade" dans stateDir.
func execInit(
	ctx context.Context,
	stateDir string,
	run CmdRunner,
	opts Options,
	log *slog.Logger,
	success, failure *int64,
) {
	if opts.DryRun {
		log.Info("[dry-run] terraform init -upgrade", "dir", stateDir)
		atomic.AddInt64(success, 1)
		return
	}

	// Vérifier le context avant de lancer la commande (le worker a pu attendre
	// longtemps dans le sémaphore).
	if ctx.Err() != nil {
		return
	}

	start := time.Now()
	out, err := run(stateDir, "terraform", "init", "-upgrade")
	elapsed := time.Since(start).Round(time.Millisecond)

	if err != nil {
		atomic.AddInt64(failure, 1)
		log.Error("terraform init échoué", "dir", stateDir, "elapsed", elapsed, "err", err, "output", string(out))
		return
	}

	atomic.AddInt64(success, 1)
	if opts.Verbose {
		log.Info("terraform init OK", "dir", stateDir, "elapsed", elapsed, "output", string(out))
	} else {
		log.Info("terraform init OK", "dir", stateDir, "elapsed", elapsed)
	}
}
