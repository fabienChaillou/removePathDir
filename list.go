package main

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// List all repositories to PATH
func ListDirectories(path string) ([]string, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, fmt.Errorf("read directory %q: %w", path, err)
	}

	directories := make([]string, 0, len(entries))

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		directories = append(
			directories,
			filepath.Join(path, entry.Name()),
		)
	}

	return directories, nil
}

func TestListDirectories(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   func(t *testing.T, root string)
		want    []string
		wantErr bool
	}{
		{
			name: "list directories",
			setup: func(t *testing.T, root string) {
				for _, name := range []string{
					"project1",
					"project2",
					"project3",
				} {
					if err := os.Mkdir(
						filepath.Join(root, name),
						0755,
					); err != nil {
						t.Fatal(err)
					}
				}

				if err := os.WriteFile(
					filepath.Join(root, "README.md"),
					[]byte("test"),
					0644,
				); err != nil {
					t.Fatal(err)
				}
			},
			want: []string{
				"project1",
				"project2",
				"project3",
			},
		},
		{
			name: "no directories",
			setup: func(t *testing.T, root string) {
				if err := os.WriteFile(
					filepath.Join(root, "file.txt"),
					[]byte("test"),
					0644,
				); err != nil {
					t.Fatal(err)
				}
			},
			want: []string{},
		},
		{
			name:  "empty directory",
			setup: func(t *testing.T, root string) {},
			want:  []string{},
		},
		{
			name: "nested directories are not returned",
			setup: func(t *testing.T, root string) {
				project := filepath.Join(root, "project1")
				nested := filepath.Join(project, "nested")

				if err := os.MkdirAll(nested, 0755); err != nil {
					t.Fatal(err)
				}
			},
			want: []string{
				"project1",
			},
		},
		{
			name:    "path does not exist",
			setup:   func(t *testing.T, root string) {},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()

			// Pour le test "path does not exist", on utilise
			// un chemin qui n'existe réellement pas.
			testPath := root

			if tt.wantErr {
				testPath = filepath.Join(root, "does-not-exist")
			}

			tt.setup(t, root)

			got, err := ListDirectories(testPath)

			if (err != nil) != tt.wantErr {
				t.Fatalf(
					"ListDirectories() error = %v, wantErr = %v",
					err,
					tt.wantErr,
				)
			}

			if tt.wantErr {
				return
			}

			want := make([]string, 0, len(tt.want))
			for _, name := range tt.want {
				want = append(want, filepath.Join(root, name))
			}

			if !reflect.DeepEqual(got, want) {
				t.Errorf(
					"ListDirectories() = %v, want %v",
					got,
					want,
				)
			}
		})
	}
}
