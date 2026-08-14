// Package initializer implémente la Phase 2 du workflow Temporal :
//  1. Récupérer la liste d'UUIDs de workspaces via GET /workspaces.
//  2. Pour chaque UUID, marcher /workspaces/{UUID} à la recherche de terraform.tfstate.
//  3. Lancer "terraform init -upgrade" dans chaque répertoire contenant un tfstate.
//
// Conception mémoire (100 000+ UUIDs) :
//   - Le body HTTP est lu avec json.Decoder token par token : aucun tableau entier
//     n'est jamais alloué, même pour une réponse de 100 000 entrées.
//   - L'API produit un <-chan string : un UUID en mémoire à la fois.
//   - filepath.WalkDir est streamé : aucune liste de chemins n'est allouée.
//   - Le pool de workers est borné par un sémaphore canal : N goroutines max.
//   - Le context Temporal est propagé partout (annulation, heartbeat).
package initializer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const tfStateFile = "terraform.tfstate"

// ── API Fetcher ───────────────────────────────────────────────────────────────

// UUIDFetcher est la signature de toute fonction capable de streamer des UUIDs
// depuis une source externe. Le channel est fermé quand tous les UUIDs ont été
// émis ou que le context est annulé.
//
// Temporal appellera cette fonction une fois ; le workflow consomme le canal
// au fil de l'eau sans jamais charger la liste complète en mémoire.
type UUIDFetcher func(ctx context.Context) (<-chan string, error)

// APIResponse est la structure JSON retournée par GET /workspaces.
// On ne l'utilise pas avec json.Unmarshal (ce qui chargerait tout en mémoire)
// mais uniquement pour documenter le contrat de l'API.
//
//	{"workspace": ["UUID1", "UUID2", ...]}
type APIResponse struct {
	Workspace []string `json:"workspace"`
}

// WorkspaceAPIFetcher retourne un UUIDFetcher qui appelle GET {baseURL}/workspaces
// et streame les UUIDs token par token depuis le body JSON.
//
// Le body HTTP n'est jamais entièrement chargé en mémoire : json.Decoder lit
// chaque token UUID individuellement et le pousse dans le channel avant de
// lire le suivant. Une réponse de 100 000 UUIDs consomme une mémoire constante.
//
// Si client est nil, http.DefaultClient est utilisé avec un timeout de 60 s.
func WorkspaceAPIFetcher(baseURL string, client *http.Client) UUIDFetcher {
	if client == nil {
		client = &http.Client{Timeout: 60 * time.Second}
	}

	endpoint := strings.TrimRight(baseURL, "/") + "/workspaces"

	return func(ctx context.Context) (<-chan string, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, fmt.Errorf("build request %s: %w", endpoint, err)
		}
		req.Header.Set("Accept", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("GET %s: %w", endpoint, err)
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, fmt.Errorf("GET %s: statut HTTP %d", endpoint, resp.StatusCode)
		}

		// Buffer de 256 : absorbe les petits pics sans jamais contenir la liste entière.
		ch := make(chan string, 256)

		go func() {
			defer close(ch)
			defer resp.Body.Close()

			if err := streamWorkspaceUUIDs(ctx, resp.Body, ch); err != nil {
				// On logue mais on ne fait pas crasher le workflow :
				// les UUIDs déjà émis seront traités normalement.
				slog.Error("stream UUIDs interrompu", "endpoint", endpoint, "err", err)
			}
		}()

		return ch, nil
	}
}

// streamWorkspaceUUIDs parse le JSON {"workspace": ["UUID1", ...]} en streaming.
//
// json.Decoder.Token() lit un seul token à la fois sans bufferiser le reste
// du document — la consommation mémoire est O(1) quelle que soit la taille du tableau.
//
// Clés inconnues dans l'objet JSON sont ignorées proprement (skip de leur valeur).
func streamWorkspaceUUIDs(ctx context.Context, r io.Reader, ch chan<- string) error {
	dec := json.NewDecoder(r)

	// Lire le '{' ouvrant.
	if err := expectDelim(dec, '{'); err != nil {
		return fmt.Errorf("ouverture objet JSON: %w", err)
	}

	// Parcourir les clés de l'objet racine.
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return fmt.Errorf("lecture clé JSON: %w", err)
		}

		key, ok := keyTok.(string)
		if !ok {
			return fmt.Errorf("clé JSON attendue, obtenu %T", keyTok)
		}

		if key != "workspace" {
			// Clé inconnue : sauter sa valeur entière (objet, tableau, scalaire).
			var skip json.RawMessage
			if err := dec.Decode(&skip); err != nil {
				return fmt.Errorf("skip clé %q: %w", key, err)
			}
			continue
		}

		// Lire le '[' ouvrant du tableau d'UUIDs.
		if err := expectDelim(dec, '['); err != nil {
			return fmt.Errorf("ouverture tableau workspace: %w", err)
		}

		// Lire chaque UUID un par un — c'est ici que réside le gain mémoire.
		for dec.More() {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			var uuid string
			if err := dec.Decode(&uuid); err != nil {
				return fmt.Errorf("décodage UUID: %w", err)
			}
			if uuid == "" {
				continue // ignorer les entrées vides
			}

			select {
			case ch <- uuid:
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		// Lire le ']' fermant (optionnel : on break de toute façon).
		if _, err := dec.Token(); err != nil && err != io.EOF {
			return fmt.Errorf("fermeture tableau workspace: %w", err)
		}
		break // on a trouvé et consommé "workspace", inutile de continuer
	}

	return nil
}

// expectDelim lit le prochain token et vérifie que c'est bien le délimiteur attendu.
func expectDelim(dec *json.Decoder, expected json.Delim) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	got, ok := tok.(json.Delim)
	if !ok || got != expected {
		return fmt.Errorf("attendu %q, obtenu %v (%T)", expected, tok, tok)
	}
	return nil
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
