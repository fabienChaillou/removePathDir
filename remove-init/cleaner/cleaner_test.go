package cleaner_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"terraform-cleaner/cleaner"
)

// mkdirs crée une arborescence de répertoires à partir de root.
func mkdirs(t *testing.T, root string, paths []string) {
	t.Helper()
	for _, p := range paths {
		full := filepath.Join(root, p)
		if err := os.MkdirAll(full, 0o755); err != nil {
			t.Fatalf("mkdirs: %v", err)
		}
	}
}

// exists retourne vrai si path existe sur le système de fichiers.
func exists(path string) bool {
	_, err := os.Lstat(path)
	return err == nil
}

// ── Tests ─────────────────────────────────────────────────────────────────────

func TestRemoveTerraformDirs_RemovesAllDotTerraform(t *testing.T) {
	root := t.TempDir()

	mkdirs(t, root, []string{
		"project-a/.terraform",
		"project-a/.terraform/providers", // contenu imbriqué
		"project-b/.terraform",
		"project-b/sub-module/.terraform",
		"project-c/src", // répertoire sans .terraform
	})

	res, err := cleaner.RemoveTerraformDirs(context.Background(), root, cleaner.Options{})
	if err != nil {
		t.Fatalf("inattendu err=%v", err)
	}

	if res.Removed != 3 {
		t.Errorf("Removed = %d ; attendu 3", res.Removed)
	}
	if res.Skipped != 0 {
		t.Errorf("Skipped = %d ; attendu 0", res.Skipped)
	}

	// Les .terraform n'existent plus…
	for _, rel := range []string{
		"project-a/.terraform",
		"project-b/.terraform",
		"project-b/sub-module/.terraform",
	} {
		if exists(filepath.Join(root, rel)) {
			t.Errorf("%s devrait avoir été supprimé", rel)
		}
	}

	// … mais le reste de l'arborescence est intact.
	if !exists(filepath.Join(root, "project-c/src")) {
		t.Error("project-c/src ne devrait pas avoir été supprimé")
	}
}

func TestRemoveTerraformDirs_DryRun_TouchesNothing(t *testing.T) {
	root := t.TempDir()
	mkdirs(t, root, []string{"project-a/.terraform", "project-b/.terraform"})

	res, err := cleaner.RemoveTerraformDirs(context.Background(), root,
		cleaner.Options{DryRun: true})
	if err != nil {
		t.Fatalf("inattendu err=%v", err)
	}

	// En dry-run, Removed reste à zéro.
	if res.Removed != 0 {
		t.Errorf("dry-run ne devrait pas incrémenter Removed, got %d", res.Removed)
	}

	// Les répertoires sont toujours là.
	for _, rel := range []string{"project-a/.terraform", "project-b/.terraform"} {
		if !exists(filepath.Join(root, rel)) {
			t.Errorf("dry-run a quand même supprimé %s", rel)
		}
	}
}

func TestRemoveTerraformDirs_EmptyRoot_ReturnsZero(t *testing.T) {
	root := t.TempDir()
	res, err := cleaner.RemoveTerraformDirs(context.Background(), root, cleaner.Options{})
	if err != nil {
		t.Fatalf("inattendu err=%v", err)
	}
	if res.Removed != 0 || res.Skipped != 0 {
		t.Errorf("arbre vide : attendu (0,0), got (%d,%d)", res.Removed, res.Skipped)
	}
}

func TestRemoveTerraformDirs_ContextCancelled_StopsEarly(t *testing.T) {
	root := t.TempDir()
	// Beaucoup de répertoires pour s'assurer que la marche ne finit pas
	// avant que le context soit annulé.
	paths := make([]string, 200)
	for i := range paths {
		paths[i] = filepath.Join(filepath.FromSlash("deep/nested/level"), string(rune('a'+i%26)))
	}
	mkdirs(t, root, paths)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // annulation immédiate

	_, err := cleaner.RemoveTerraformDirs(ctx, root, cleaner.Options{})
	if err == nil {
		t.Error("attendu une erreur de context annulé, got nil")
	}
}

func TestRemoveTerraformDirs_SkipsInaccessibleEntries(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("test non pertinent en root : les permissions ne bloquent pas")
	}

	root := t.TempDir()
	mkdirs(t, root, []string{"ok/.terraform", "restricted"})

	// Rendre restricted inaccessible.
	restricted := filepath.Join(root, "restricted")
	if err := os.Chmod(restricted, 0o000); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { os.Chmod(restricted, 0o755) })

	res, err := cleaner.RemoveTerraformDirs(context.Background(), root, cleaner.Options{})
	if err != nil {
		t.Fatalf("inattendu err=%v", err)
	}

	// "ok/.terraform" a été supprimé.
	if res.Removed != 1 {
		t.Errorf("Removed = %d ; attendu 1", res.Removed)
	}
	// L'entrée inaccessible a été comptée comme Skipped.
	if res.Skipped < 1 {
		t.Errorf("Skipped = %d ; attendu >= 1", res.Skipped)
	}
}
