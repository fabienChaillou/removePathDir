// Package cleaner implémente la Phase 1 du workflow Temporal :
// suppression récursive de tous les répertoires ".terraform" sous une racine donnée.
//
// Conception mémoire :
//   - filepath.WalkDir est entièrement streamé — aucune liste n'est chargée en RAM.
//   - fs.SkipDir est retourné dès qu'un .terraform est trouvé : la descente
//     dans ce répertoire est annulée, évitant tout travail inutile.
//   - Le context est vérifié à chaque entrée pour permettre une annulation
//     propre depuis Temporal (heartbeat timeout, cancel signal…).
package cleaner

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
)

const dotTerraform = ".terraform"

// Options configure le comportement du nettoyeur.
type Options struct {
	// DryRun affiche les actions sans les exécuter (défaut : false).
	DryRun bool

	// Logger utilisé pour les traces (défaut : slog.Default()).
	Logger *slog.Logger
}

func (o Options) logger() *slog.Logger {
	if o.Logger != nil {
		return o.Logger
	}
	return slog.Default()
}

// Result résume ce qui a été fait.
type Result struct {
	Removed int64 // répertoires supprimés avec succès
	Skipped int64 // entrées ignorées à cause d'une erreur d'accès ou de suppression
}

func (r Result) String() string {
	return fmt.Sprintf("removed=%d skipped=%d", r.Removed, r.Skipped)
}

// RemoveTerraformDirs parcourt root et supprime chaque répertoire ".terraform"
// qu'il rencontre. Temporal peut annuler l'opération via ctx.
//
// Les erreurs d'accès aux entrées individuelles sont loguées et ignorées
// (la marche continue) ; seules les erreurs fatales sur root lui-même
// font échouer la fonction.
func RemoveTerraformDirs(ctx context.Context, root string, opts Options) (Result, error) {
	log := opts.logger()

	var res Result

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		// Vérification du context à chaque nœud — annulation Temporal propre.
		if err := ctx.Err(); err != nil {
			return err
		}

		if walkErr != nil {
			log.Warn("accès impossible", "path", path, "err", walkErr)
			atomic.AddInt64(&res.Skipped, 1)
			return nil // on continue malgré l'erreur
		}

		if !d.IsDir() || d.Name() != dotTerraform {
			return nil
		}

		if opts.DryRun {
			log.Info("[dry-run] supprimerais", "path", path)
			// En dry-run on n'incrémente pas Removed, mais on saute quand même
			// la descente : rien à inspecter dans .terraform.
			return fs.SkipDir
		}

		if err := os.RemoveAll(path); err != nil {
			log.Error("échec suppression", "path", path, "err", err)
			atomic.AddInt64(&res.Skipped, 1)
		} else {
			log.Info("supprimé", "path", path)
			atomic.AddInt64(&res.Removed, 1)
		}

		return fs.SkipDir // ne pas descendre dans .terraform
	})

	// Si l'erreur vient du context (annulation Temporal), on la remonte.
	if ctx.Err() != nil {
		return res, ctx.Err()
	}
	return res, err
}
