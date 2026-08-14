package initializer_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"terraform-cleaner/initializer"
)

// ── Helpers ───────────────────────────────────────────────────────────────────

// mkWorkspace crée /root/{uuid}/terraform.tfstate (et éventuellement des sous-modules).
func mkWorkspace(t *testing.T, root, uuid string, subDirs []string) {
	t.Helper()
	base := filepath.Join(root, uuid)
	if err := os.MkdirAll(base, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, "terraform.tfstate"), []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, sub := range subDirs {
		dir := filepath.Join(base, sub)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "terraform.tfstate"), []byte("{}"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// staticFetcher retourne un UUIDFetcher qui émet les UUIDs fournis, dans l'ordre.
func staticFetcher(uuids ...string) initializer.UUIDFetcher {
	return func(ctx context.Context) (<-chan string, error) {
		ch := make(chan string, len(uuids))
		for _, u := range uuids {
			ch <- u
		}
		close(ch)
		return ch, nil
	}
}

// recordingRunner est un CmdRunner de test qui enregistre les répertoires appelés
// et peut simuler des échecs ciblés.
type recordingRunner struct {
	calls   atomic.Int64
	dirs    []string // protégé par le fait que les tests vérifient après wg.Wait
	failFor map[string]bool
	mu      chan struct{} // mutex léger
}

func newRecordingRunner(failFor ...string) *recordingRunner {
	fails := make(map[string]bool, len(failFor))
	for _, f := range failFor {
		fails[f] = true
	}
	r := &recordingRunner{failFor: fails, mu: make(chan struct{}, 1)}
	r.mu <- struct{}{}
	return r
}

func (r *recordingRunner) Run(dir, _ string, _ ...string) ([]byte, error) {
	r.calls.Add(1)

	<-r.mu
	r.dirs = append(r.dirs, dir)
	r.mu <- struct{}{}

	if r.failFor[dir] {
		return nil, fmt.Errorf("simulated terraform error")
	}
	return []byte("Terraform has been successfully initialized!"), nil
}

func (r *recordingRunner) Called() int64  { return r.calls.Load() }
func (r *recordingRunner) Dirs() []string { <-r.mu; d := r.dirs; r.mu <- struct{}{}; return d }

// ── Tests SimulatedAPIFetcher ─────────────────────────────────────────────────

func TestSimulatedAPIFetcher_StreamsAllUUIDs(t *testing.T) {
	fetch := initializer.SimulatedAPIFetcher(10, 3) // 3 pages × 10 = 30 UUIDs
	ch, err := fetch(context.Background())
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}

	var count int
	for uuid := range ch {
		if uuid == "" {
			t.Error("UUID vide reçu")
		}
		count++
	}
	if count != 30 {
		t.Errorf("attendu 30 UUIDs, reçu %d", count)
	}
}

func TestSimulatedAPIFetcher_RespectsContextCancellation(t *testing.T) {
	fetch := initializer.SimulatedAPIFetcher(1000, 100) // 100 000 UUIDs théoriques
	ctx, cancel := context.WithCancel(context.Background())

	ch, err := fetch(ctx)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}

	// Lire quelques UUIDs puis annuler.
	for i := 0; i < 5; i++ {
		<-ch
	}
	cancel()

	// Le channel doit se fermer rapidement après annulation.
	done := make(chan struct{})
	go func() {
		for range ch {
		}
		close(done)
	}()

	select {
	case <-done:
		// OK — le fetcher a honoré l'annulation.
	case <-time.After(2 * time.Second):
		t.Fatal("le fetcher n'a pas respecté l'annulation du context dans les temps")
	}
}

func TestSimulatedAPIFetcher_UniqueUUIDs(t *testing.T) {
	fetch := initializer.SimulatedAPIFetcher(5, 4) // 20 UUIDs
	ch, _ := fetch(context.Background())

	seen := make(map[string]struct{})
	for uuid := range ch {
		if _, dup := seen[uuid]; dup {
			t.Errorf("UUID dupliqué : %s", uuid)
		}
		seen[uuid] = struct{}{}
	}
}

// ── Tests RunTerraformInit ────────────────────────────────────────────────────

func TestRunTerraformInit_CallsInitForEachTfState(t *testing.T) {
	root := t.TempDir()
	mkWorkspace(t, root, "uuid-001", nil)
	mkWorkspace(t, root, "uuid-002", nil)
	mkWorkspace(t, root, "uuid-003", nil)

	rec := newRecordingRunner()
	res, err := initializer.RunTerraformInit(
		context.Background(),
		staticFetcher("uuid-001", "uuid-002", "uuid-003"),
		initializer.Options{
			WorkspacesRoot: root,
			Workers:        2,
			RunCommand:     rec.Run,
		},
	)
	if err != nil {
		t.Fatalf("inattendu err=%v", err)
	}

	if res.UUIDs != 3 {
		t.Errorf("UUIDs = %d ; attendu 3", res.UUIDs)
	}
	if res.Found != 3 {
		t.Errorf("Found = %d ; attendu 3", res.Found)
	}
	if res.Success != 3 {
		t.Errorf("Success = %d ; attendu 3", res.Success)
	}
	if res.Failure != 0 {
		t.Errorf("Failure = %d ; attendu 0", res.Failure)
	}
	if rec.Called() != 3 {
		t.Errorf("terraform appelé %d fois ; attendu 3", rec.Called())
	}
}

func TestRunTerraformInit_SubModules_FindsMultipleTfState(t *testing.T) {
	root := t.TempDir()
	// uuid-001 a un tfstate racine + deux sous-modules.
	mkWorkspace(t, root, "uuid-001", []string{"module-a", "module-b"})

	rec := newRecordingRunner()
	res, err := initializer.RunTerraformInit(
		context.Background(),
		staticFetcher("uuid-001"),
		initializer.Options{WorkspacesRoot: root, Workers: 4, RunCommand: rec.Run},
	)
	if err != nil {
		t.Fatalf("inattendu err=%v", err)
	}

	// 3 tfstate : racine + module-a + module-b
	if res.Found != 3 {
		t.Errorf("Found = %d ; attendu 3", res.Found)
	}
	if res.Success != 3 {
		t.Errorf("Success = %d ; attendu 3", res.Success)
	}
}

func TestRunTerraformInit_DryRun_NeverCallsTerraform(t *testing.T) {
	root := t.TempDir()
	mkWorkspace(t, root, "uuid-001", nil)
	mkWorkspace(t, root, "uuid-002", nil)

	rec := newRecordingRunner()
	res, err := initializer.RunTerraformInit(
		context.Background(),
		staticFetcher("uuid-001", "uuid-002"),
		initializer.Options{
			WorkspacesRoot: root,
			Workers:        2,
			DryRun:         true,
			RunCommand:     rec.Run,
		},
	)
	if err != nil {
		t.Fatalf("inattendu err=%v", err)
	}

	if rec.Called() != 0 {
		t.Errorf("dry-run : terraform ne devrait pas être appelé, got %d appels", rec.Called())
	}
	// Success est quand même incrémenté en dry-run (l'action serait un succès).
	if res.Success != 2 {
		t.Errorf("dry-run Success = %d ; attendu 2", res.Success)
	}
}

func TestRunTerraformInit_PartialFailure_CountedCorrectly(t *testing.T) {
	root := t.TempDir()
	mkWorkspace(t, root, "uuid-ok", nil)
	mkWorkspace(t, root, "uuid-fail", nil)

	failDir := filepath.Join(root, "uuid-fail")
	rec := newRecordingRunner(failDir)

	res, err := initializer.RunTerraformInit(
		context.Background(),
		staticFetcher("uuid-ok", "uuid-fail"),
		initializer.Options{WorkspacesRoot: root, Workers: 1, RunCommand: rec.Run},
	)
	if err != nil {
		t.Fatalf("inattendu err=%v", err)
	}

	if res.Success != 1 {
		t.Errorf("Success = %d ; attendu 1", res.Success)
	}
	if res.Failure != 1 {
		t.Errorf("Failure = %d ; attendu 1", res.Failure)
	}
}

func TestRunTerraformInit_MissingWorkspace_Skipped(t *testing.T) {
	root := t.TempDir()
	mkWorkspace(t, root, "uuid-exists", nil)
	// "uuid-ghost" n'existe pas sur le disque.

	rec := newRecordingRunner()
	res, err := initializer.RunTerraformInit(
		context.Background(),
		staticFetcher("uuid-exists", "uuid-ghost"),
		initializer.Options{WorkspacesRoot: root, Workers: 2, RunCommand: rec.Run},
	)
	if err != nil {
		t.Fatalf("inattendu err=%v", err)
	}

	// Seul uuid-exists a un tfstate.
	if res.Found != 1 {
		t.Errorf("Found = %d ; attendu 1", res.Found)
	}
	if rec.Called() != 1 {
		t.Errorf("terraform appelé %d fois ; attendu 1", rec.Called())
	}
}

func TestRunTerraformInit_ContextCancelled_StopsEarly(t *testing.T) {
	root := t.TempDir()
	// Créer beaucoup de workspaces.
	uuids := make([]string, 50)
	for i := range uuids {
		uuids[i] = fmt.Sprintf("uuid-%03d", i)
		mkWorkspace(t, root, uuids[i], nil)
	}

	ctx, cancel := context.WithCancel(context.Background())

	var called atomic.Int64
	slowRunner := func(dir, _ string, _ ...string) ([]byte, error) {
		called.Add(1)
		// Simuler un init lent.
		time.Sleep(50 * time.Millisecond)
		return nil, nil
	}

	// Annuler après un court délai.
	go func() {
		time.Sleep(80 * time.Millisecond)
		cancel()
	}()

	res, err := initializer.RunTerraformInit(
		ctx,
		staticFetcher(uuids...),
		initializer.Options{WorkspacesRoot: root, Workers: 2, RunCommand: slowRunner},
	)

	if err == nil {
		t.Error("attendu une erreur de context, got nil")
	}
	// Moins de 50 terraform init ont été lancés (annulation précoce).
	if res.Found >= 50 {
		t.Errorf("annulation ignorée : Found=%d, attendu < 50", res.Found)
	}
}

func TestRunTerraformInit_WorkersArg_PassedToTerraform(t *testing.T) {
	root := t.TempDir()
	for i := 0; i < 10; i++ {
		mkWorkspace(t, root, fmt.Sprintf("uuid-%02d", i), nil)
	}

	var maxConcurrent atomic.Int64
	var current atomic.Int64

	concurrentRunner := func(dir, name string, args ...string) ([]byte, error) {
		c := current.Add(1)
		if c > maxConcurrent.Load() {
			maxConcurrent.Store(c)
		}
		time.Sleep(30 * time.Millisecond)
		current.Add(-1)
		// Vérifier que la commande est bien "terraform init -upgrade".
		if name != "terraform" {
			return nil, fmt.Errorf("commande inattendue : %s", name)
		}
		if len(args) < 2 || args[0] != "init" || args[1] != "-upgrade" {
			return nil, fmt.Errorf("args inattendus : %v", args)
		}
		return nil, nil
	}

	uuids := make([]string, 10)
	for i := range uuids {
		uuids[i] = fmt.Sprintf("uuid-%02d", i)
	}

	_, err := initializer.RunTerraformInit(
		context.Background(),
		staticFetcher(uuids...),
		initializer.Options{WorkspacesRoot: root, Workers: 3, RunCommand: concurrentRunner},
	)
	if err != nil {
		t.Fatalf("inattendu err=%v", err)
	}

	// Le parallélisme n'a jamais dépassé Workers=3.
	if maxConcurrent.Load() > 3 {
		t.Errorf("parallélisme max = %d ; attendu <= 3", maxConcurrent.Load())
	}
}

func TestRunTerraformInit_NoTfState_NothingCalled(t *testing.T) {
	root := t.TempDir()
	// Workspace sans tfstate.
	if err := os.MkdirAll(filepath.Join(root, "uuid-empty"), 0o755); err != nil {
		t.Fatal(err)
	}

	rec := newRecordingRunner()
	res, err := initializer.RunTerraformInit(
		context.Background(),
		staticFetcher("uuid-empty"),
		initializer.Options{WorkspacesRoot: root, Workers: 2, RunCommand: rec.Run},
	)
	if err != nil {
		t.Fatalf("inattendu err=%v", err)
	}
	if res.Found != 0 {
		t.Errorf("Found = %d ; attendu 0", res.Found)
	}
	if rec.Called() != 0 {
		t.Errorf("terraform appelé %d fois sur un workspace sans tfstate", rec.Called())
	}
}

func TestRunTerraformInit_CommandArgs_AreCorrect(t *testing.T) {
	root := t.TempDir()
	mkWorkspace(t, root, "uuid-check", nil)

	var capturedArgs []string
	checkRunner := func(dir, name string, args ...string) ([]byte, error) {
		capturedArgs = append([]string{name}, args...)
		return nil, nil
	}

	_, err := initializer.RunTerraformInit(
		context.Background(),
		staticFetcher("uuid-check"),
		initializer.Options{WorkspacesRoot: root, Workers: 1, RunCommand: checkRunner},
	)
	if err != nil {
		t.Fatalf("inattendu err=%v", err)
	}

	expected := []string{"terraform", "init", "-upgrade"}
	if strings.Join(capturedArgs, " ") != strings.Join(expected, " ") {
		t.Errorf("commande = %v ; attendu %v", capturedArgs, expected)
	}
}
