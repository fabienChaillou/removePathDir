package initializer_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
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

// ── Helpers httptest ──────────────────────────────────────────────────────────

// jsonWorkspaceServer démarre un serveur httptest qui répond à GET /workspaces
// avec {"workspace": [uuids...]}. Il est automatiquement fermé en fin de test.
func jsonWorkspaceServer(t *testing.T, uuids []string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/workspaces" || r.Method != http.MethodGet {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"workspace":[`))
		for i, u := range uuids {
			if i > 0 {
				w.Write([]byte(","))
			}
			fmt.Fprintf(w, `%q`, u)
		}
		w.Write([]byte(`]}`))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// slowStreamServer écrit la réponse UUID par UUID avec une pause entre chaque,
// pour tester que le streaming ne bloque pas et que l'annulation fonctionne.
func slowStreamServer(t *testing.T, total int, delay time.Duration) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming non supporté", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Write([]byte(`{"workspace":[`))
		flusher.Flush()
		for i := 0; i < total; i++ {
			if i > 0 {
				w.Write([]byte(","))
			}
			fmt.Fprintf(w, `"uuid-%06d"`, i)
			flusher.Flush()
			time.Sleep(delay)
		}
		w.Write([]byte(`]}`))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// ── Tests WorkspaceAPIFetcher ─────────────────────────────────────────────────

func TestWorkspaceAPIFetcher_StreamsAllUUIDs(t *testing.T) {
	want := []string{"aaa-111", "bbb-222", "ccc-333"}
	srv := jsonWorkspaceServer(t, want)

	fetch := initializer.WorkspaceAPIFetcher(srv.URL, nil)
	ch, err := fetch(context.Background())
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}

	var got []string
	for uuid := range ch {
		got = append(got, uuid)
	}

	if len(got) != len(want) {
		t.Fatalf("reçu %d UUIDs, attendu %d", len(got), len(want))
	}
	for i, u := range want {
		if got[i] != u {
			t.Errorf("[%d] attendu %q, got %q", i, u, got[i])
		}
	}
}

func TestWorkspaceAPIFetcher_LargeResponse_StreamedWithoutFullLoad(t *testing.T) {
	// 10 000 UUIDs — vérifie que tout arrive et que rien n'est perdu.
	const total = 10_000
	uuids := make([]string, total)
	for i := range uuids {
		uuids[i] = fmt.Sprintf("uuid-%06d", i)
	}
	srv := jsonWorkspaceServer(t, uuids)

	fetch := initializer.WorkspaceAPIFetcher(srv.URL, nil)
	ch, err := fetch(context.Background())
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}

	var count int
	for range ch {
		count++
	}
	if count != total {
		t.Errorf("reçu %d UUIDs, attendu %d", count, total)
	}
}

func TestWorkspaceAPIFetcher_ContextCancellation_StopsStream(t *testing.T) {
	// Serveur qui écrit lentement — l'annulation doit interrompre la lecture.
	srv := slowStreamServer(t, 500, 5*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	fetch := initializer.WorkspaceAPIFetcher(srv.URL, nil)

	ch, err := fetch(ctx)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}

	// Consommer quelques UUIDs puis annuler.
	for i := 0; i < 3; i++ {
		<-ch
	}
	cancel()

	// Le channel doit se fermer rapidement.
	done := make(chan struct{})
	go func() {
		for range ch {
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("le channel n'a pas été fermé après annulation du context")
	}
}

func TestWorkspaceAPIFetcher_HTTPError_ReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "service indisponible", http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)

	fetch := initializer.WorkspaceAPIFetcher(srv.URL, nil)
	_, err := fetch(context.Background())
	if err == nil {
		t.Fatal("attendu une erreur sur statut 503, got nil")
	}
	if !strings.Contains(err.Error(), "503") {
		t.Errorf("message d'erreur devrait contenir '503', got: %v", err)
	}
}

func TestWorkspaceAPIFetcher_UnknownJSONKeys_Ignored(t *testing.T) {
	// L'API renvoie des clés supplémentaires — elles doivent être ignorées.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"meta":{"page":1},"workspace":["id-aaa","id-bbb"],"total":2}`))
	}))
	t.Cleanup(srv.Close)

	fetch := initializer.WorkspaceAPIFetcher(srv.URL, nil)
	ch, err := fetch(context.Background())
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}

	var got []string
	for uuid := range ch {
		got = append(got, uuid)
	}
	if len(got) != 2 || got[0] != "id-aaa" || got[1] != "id-bbb" {
		t.Errorf("UUIDs incorrects : %v", got)
	}
}

func TestWorkspaceAPIFetcher_EmptyWorkspaceArray_NoUUIDs(t *testing.T) {
	srv := jsonWorkspaceServer(t, []string{})

	fetch := initializer.WorkspaceAPIFetcher(srv.URL, nil)
	ch, err := fetch(context.Background())
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}

	var count int
	for range ch {
		count++
	}
	if count != 0 {
		t.Errorf("attendu 0 UUIDs, reçu %d", count)
	}
}

func TestWorkspaceAPIFetcher_InvalidBaseURL_ReturnsError(t *testing.T) {
	fetch := initializer.WorkspaceAPIFetcher("http://127.0.0.1:0", nil) // port fermé
	_, err := fetch(context.Background())
	if err == nil {
		t.Fatal("attendu une erreur de connexion, got nil")
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
