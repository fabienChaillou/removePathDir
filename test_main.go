package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCleanTerraformDir(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		setup         func(t *testing.T, basePath string)
		wantErr       bool
		wantTerraform bool
	}{
		{
			name: "terraform directory does not exist",
			setup: func(t *testing.T, basePath string) {
				// Rien à créer
			},
			wantErr:       false,
			wantTerraform: false,
		},
		{
			name: "terraform directory is deleted",
			setup: func(t *testing.T, basePath string) {
				terraformDir := filepath.Join(basePath, ".terraform")

				if err := os.Mkdir(terraformDir, 0755); err != nil {
					t.Fatal(err)
				}
			},
			wantErr:       false,
			wantTerraform: false,
		},
		{
			name: "terraform directory with content is deleted",
			setup: func(t *testing.T, basePath string) {
				terraformDir := filepath.Join(basePath, ".terraform")

				if err := os.MkdirAll(
					filepath.Join(terraformDir, "providers", "registry"),
					0755,
				); err != nil {
					t.Fatal(err)
				}

				file := filepath.Join(
					terraformDir,
					"terraform.tfstate",
				)

				if err := os.WriteFile(
					file,
					[]byte("test"),
					0644,
				); err != nil {
					t.Fatal(err)
				}
			},
			wantErr:       false,
			wantTerraform: false,
		},
		{
			name: "terraform path is a file",
			setup: func(t *testing.T, basePath string) {
				terraformPath := filepath.Join(basePath, ".terraform")

				if err := os.WriteFile(
					terraformPath,
					[]byte("not a directory"),
					0644,
				); err != nil {
					t.Fatal(err)
				}
			},
			wantErr:       true,
			wantTerraform: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			basePath := t.TempDir()

			tt.setup(t, basePath)

			err := cleanTerraformDir(basePath)

			if (err != nil) != tt.wantErr {
				t.Fatalf(
					"cleanTerraformDir() error = %v, wantErr = %v",
					err,
					tt.wantErr,
				)
			}

			terraformPath := filepath.Join(basePath, ".terraform")

			_, err = os.Stat(terraformPath)

			exists := err == nil

			if exists != tt.wantTerraform {
				t.Fatalf(
					".terraform exists = %v, want = %v",
					exists,
					tt.wantTerraform,
				)
			}
		})
	}
}
