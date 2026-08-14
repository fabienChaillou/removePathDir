// Package initializer_test — exemples d'intégration et benchmarks mémoire.
//
// Lancez les benchmarks ainsi pour voir les allocations réelles :
//
//	go test ./initializer/... -run=^$ -bench=. -benchmem -v
package initializer_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"terraform-cleaner/initializer"
)

// discardLogger retourne un slog.Logger qui jette toutes les traces.
// Utilisé dans les benchmarks et tests mémoire pour ne pas fausser
// les mesures avec les allocations du formateur de logs.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// ── Serveur HTTP générateur (streaming, sans allocation côté serveur) ─────────

// streamingWorkspaceServer démarre un serveur httptest dont le handler
// génère les UUIDs à la volée sans jamais construire le tableau complet.
//
// Pourquoi ce pattern est important :
//   - Si on appelait json.Marshal([]string{...100k...}) côté serveur, on
//     allouerait ~5 Mo pour la sérialisation seule.
//   - Ici, on écrit token par token avec io.WriteString + Flush : la mémoire
//     serveur est aussi O(1), ce qui simule une vraie API bien conçue.
func streamingWorkspaceServer(total int) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/workspaces" {
			http.NotFound(w, r)
			return
		}

		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming requis", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		// Transfer-Encoding: chunked est automatique dès qu'on Flush avant de terminer.

		io.WriteString(w, `{"workspace":[`)
		flusher.Flush()

		for i := 0; i < total; i++ {
			if i > 0 {
				io.WriteString(w, ",")
			}
			// Chaque UUID est écrit directement dans le réseau — pas de buffer global.
			fmt.Fprintf(w, `"ws-%08d"`, i)

			// On flush toutes les 500 entrées pour que le client reçoive
			// les données en continu sans attendre la fin de la réponse.
			if i%500 == 0 {
				flusher.Flush()
			}
		}

		io.WriteString(w, `]}`)
		flusher.Flush()
	}))
}

// ── Helpers de test d'intégration ─────────────────────────────────────────────

// buildWorkspaces crée une arborescence de workspaces factices sous root
// pour les n premiers UUIDs (format "ws-XXXXXXXX").
func buildWorkspaces(t *testing.T, root string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		dir := filepath.Join(root, fmt.Sprintf("ws-%08d", i))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "terraform.tfstate"), []byte("{}"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// noopRunner remplace "terraform init" par une no-op instantanée.
// Indispensable dans les tests d'intégration : on veut mesurer le pipeline,
// pas attendre terraform.
func noopRunner(_, _ string, _ ...string) ([]byte, error) {
	return nil, nil
}

// ── Exemple canonique de câblage ──────────────────────────────────────────────
//
// ExampleWorkspaceAPIFetcher montre le câblage minimal entre les deux composants.
// Dans un workflow Temporal, chaque phase est une Activity séparée ; ici on les
// enchaîne manuellement pour illustrer le flot de données.

func ExampleWorkspaceAPIFetcher() {
	// 1. Démarrer un serveur fictif (en production : remplacer par l'URL réelle).
	srv := streamingWorkspaceServer(3)
	defer srv.Close()

	root, _ := os.MkdirTemp("", "ws-example-*")
	defer os.RemoveAll(root)
	for _, uuid := range []string{"ws-00000000", "ws-00000001", "ws-00000002"} {
		dir := filepath.Join(root, uuid)
		os.MkdirAll(dir, 0o755)
		os.WriteFile(filepath.Join(dir, "terraform.tfstate"), []byte("{}"), 0o644)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// ── Point de câblage ──────────────────────────────────────────────────────
	//
	// WorkspaceAPIFetcher retourne un UUIDFetcher (= func(ctx) (<-chan string, error)).
	// Ce UUIDFetcher est passé directement à RunTerraformInit — les deux fonctions
	// ne se connaissent que via cette interface, ce qui permet de les tester et
	// déployer indépendamment dans Temporal.

	fetch := initializer.WorkspaceAPIFetcher(srv.URL, &http.Client{
		// Timeout long pour les grandes réponses (100k UUIDs peuvent prendre
		// plusieurs minutes selon la bande passante réseau).
		Timeout: 10 * time.Minute,
	})

	res, err := initializer.RunTerraformInit(ctx, fetch, initializer.Options{
		WorkspacesRoot: root,
		Workers:        4,    // 4 "terraform init" parallèles max
		DryRun:         true, // ne pas appeler terraform dans cet exemple
	})
	if err != nil {
		fmt.Printf("erreur: %v\n", err)
		return
	}

	fmt.Printf("uuids=%d found=%d success=%d failure=%d\n",
		res.UUIDs, res.Found, res.Success, res.Failure)
	// Output:
	// uuids=3 found=3 success=3 failure=0
}

// ── Test d'intégration 1 000 UUIDs ───────────────────────────────────────────

func TestIntegration_1000UUIDs_PipelineIsCorrect(t *testing.T) {
	const total = 1_000

	srv := streamingWorkspaceServer(total)
	defer srv.Close()

	root := t.TempDir()
	buildWorkspaces(t, root, total)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	fetch := initializer.WorkspaceAPIFetcher(srv.URL, nil)

	res, err := initializer.RunTerraformInit(ctx, fetch, initializer.Options{
		WorkspacesRoot: root,
		Workers:        8,
		RunCommand:     noopRunner,
	})
	if err != nil {
		t.Fatalf("erreur pipeline: %v", err)
	}

	if res.UUIDs != total {
		t.Errorf("UUIDs consommés = %d ; attendu %d", res.UUIDs, total)
	}
	if res.Found != total {
		t.Errorf("tfstate trouvés = %d ; attendu %d", res.Found, total)
	}
	if res.Success != total {
		t.Errorf("succès = %d ; attendu %d", res.Success, total)
	}
	if res.Failure != 0 {
		t.Errorf("échecs = %d ; attendu 0", res.Failure)
	}
}

// ── Benchmarks mémoire ────────────────────────────────────────────────────────
//
// Ces benchmarks mesurent les allocations TOTALES de la chaîne complète :
// HTTP fetch → décodage JSON → channel → WalkDir → noop runner.
//
// Si l'architecture est vraiment O(1) en mémoire, les colonnes "allocs/op"
// et "B/op" ne doivent PAS croître proportionnellement au nombre d'UUIDs.
//
// Lancez avec :
//
//	go test ./initializer/... -run=^$ -bench=BenchmarkPipeline -benchmem -count=3

func benchmarkPipeline(b *testing.B, total int) {
	b.Helper()

	srv := streamingWorkspaceServer(total)
	defer srv.Close()

	// Créer les workspaces une seule fois hors de la boucle de benchmark.
	root := b.TempDir()
	for i := 0; i < total; i++ {
		dir := filepath.Join(root, fmt.Sprintf("ws-%08d", i))
		os.MkdirAll(dir, 0o755)
		os.WriteFile(filepath.Join(dir, "terraform.tfstate"), []byte("{}"), 0o644)
	}

	opts := initializer.Options{
		WorkspacesRoot: root,
		Workers:        runtime.NumCPU(),
		RunCommand:     noopRunner,
		Logger:         discardLogger(),
	}

	b.ResetTimer()
	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		ctx := context.Background()
		fetch := initializer.WorkspaceAPIFetcher(srv.URL, nil)

		res, err := initializer.RunTerraformInit(ctx, fetch, opts)
		if err != nil {
			b.Fatalf("erreur: %v", err)
		}
		if res.UUIDs != int64(total) {
			b.Fatalf("UUIDs attendus %d, reçus %d", total, res.UUIDs)
		}
	}
}

// BenchmarkPipeline_1k / 10k / 100k permettent de comparer les colonnes
// B/op et allocs/op : si elles restent dans le même ordre de grandeur
// malgré un facteur 100 sur le nombre d'UUIDs, la mémoire est O(1).

func BenchmarkPipeline_1k(b *testing.B)   { benchmarkPipeline(b, 1_000) }
func BenchmarkPipeline_10k(b *testing.B)  { benchmarkPipeline(b, 10_000) }
func BenchmarkPipeline_100k(b *testing.B) { benchmarkPipeline(b, 100_000) }

// ── Vérification de la pression mémoire réelle ────────────────────────────────
//
// Ce test mesure l'empreinte mémoire AVANT et APRÈS le traitement de 50 000 UUIDs
// et vérifie que le delta HeapInuse reste sous un seuil raisonnable.
//
// HeapInuse = mémoire effectivement utilisée par le heap Go (tas vivant).
// On tolère 64 Mo pour la totalité du pipeline — en pratique on observera
// bien moins car la GC peut collecter au fil de l'eau.

func TestMemoryPressure_50kUUIDs_HeapStaysFlat(t *testing.T) {
	const (
		total        = 50_000
		maxHeapDelta = 64 << 20 // 64 MiB — seuil volontairement généreux
	)

	srv := streamingWorkspaceServer(total)
	defer srv.Close()

	root := t.TempDir()
	for i := 0; i < total; i++ {
		dir := filepath.Join(root, fmt.Sprintf("ws-%08d", i))
		os.MkdirAll(dir, 0o755)
		os.WriteFile(filepath.Join(dir, "terraform.tfstate"), []byte("{}"), 0o644)
	}

	// Forcer un GC complet pour avoir une baseline propre.
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	fetch := initializer.WorkspaceAPIFetcher(srv.URL, nil)
	res, err := initializer.RunTerraformInit(ctx, fetch, initializer.Options{
		WorkspacesRoot: root,
		Workers:        runtime.NumCPU(),
		RunCommand:     noopRunner,
		Logger:         discardLogger(), // silence les logs pour ne pas biaiser les mesures
	})
	if err != nil {
		t.Fatalf("pipeline: %v", err)
	}
	if res.UUIDs != total {
		t.Fatalf("UUIDs attendus %d, reçus %d", total, res.UUIDs)
	}

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	// HeapInuse peut être inférieur à HeapSys (mémoire rendue à l'OS).
	// On compare HeapInuse pour mesurer ce qui est réellement utilisé.
	delta := int64(after.HeapInuse) - int64(before.HeapInuse)

	t.Logf("HeapInuse avant : %s", fmtBytes(before.HeapInuse))
	t.Logf("HeapInuse après : %s", fmtBytes(after.HeapInuse))
	t.Logf("Delta            : %+d octets (%s)", delta, fmtBytes(uint64(abs(delta))))
	t.Logf("TotalAlloc       : %s (allocations cumulées, incluant les objets collectés)", fmtBytes(after.TotalAlloc-before.TotalAlloc))
	t.Logf("NumGC            : %d cycles GC pendant le pipeline", after.NumGC-before.NumGC)

	if delta > maxHeapDelta {
		t.Errorf("delta HeapInuse = %s > seuil %s : le pipeline accumule trop de mémoire",
			fmtBytes(uint64(delta)), fmtBytes(maxHeapDelta))
	}
}

// fmtBytes affiche un nombre d'octets en format lisible (KiB / MiB / GiB).
func fmtBytes(b uint64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.2f GiB", float64(b)/(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.2f MiB", float64(b)/(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.2f KiB", float64(b)/(1<<10))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

func abs(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}
