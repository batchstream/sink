package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCredentialFiles(t *testing.T) {
	for _, driver := range []string{"mongodb", "elasticsearch", "opensearch"} {
		t.Run(driver, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "value")
			secret := "  test:$with#quotes\"'\\and\nunicode-雪  \n"
			if err := os.WriteFile(path, []byte(secret), 0600); err != nil {
				t.Fatal(err)
			}
			var section string
			if driver == "mongodb" {
				section = fmt.Sprintf("mongodb:\n    uri_file: %q", path)
			} else {
				section = fmt.Sprintf("search:\n    endpoints: [http://search:9200]\n    username_file: %q\n    password_file: %q", path, path)
			}
			input := fmt.Sprintf("name: test\nstorage:\n  driver: %s\n  %s\n", driver, section)
			reader := strings.NewReader(input)
			loaded, err := Decode(strings.NewReader("mode: engine"), reader)
			if err != nil {
				t.Fatal(err)
			}
			if driver == "mongodb" {
				if loaded.Storage.MongoDB.URI != secret {
					t.Fatal("URI bytes changed")
				}
			} else if loaded.Storage.Search.Username != secret || loaded.Storage.Search.Password != secret {
				t.Fatal("basic authentication bytes changed")
			}
			if driver != "mongodb" {
				input = fmt.Sprintf("name: test\nstorage:\n  driver: %s\n  search:\n    endpoints: [http://search:9200]\n    api_key_file: %q\n", driver, path)
				reader = strings.NewReader(input)
				loaded, err = Decode(strings.NewReader("mode: engine"), reader)
				if err != nil || loaded.Storage.Search.APIKey != secret {
					t.Fatal("API key file was not resolved correctly")
				}
			}
		})
	}
}

func TestCredentialFileValidation(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "value")
	for _, test := range []struct {
		name string
		data []byte
	}{
		{name: "empty"},
		{name: "oversized", data: []byte(strings.Repeat("x", maxSecretFileBytes+1))},
		{name: "nul", data: []byte("hidden-secret\x00")},
		{name: "invalid UTF-8", data: []byte{0xff}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := os.WriteFile(path, test.data, 0600); err != nil {
				t.Fatal(err)
			}
			_, err := resolveSecret("storage.search.password", nil, &path)
			if err == nil || !strings.Contains(err.Error(), "storage.search.password_file") || strings.Contains(err.Error(), "hidden-secret") {
				t.Fatal("expected a field-specific error without credential contents")
			}
		})
	}
	for _, invalid := range []string{"", "relative", directory, filepath.Join(directory, "missing")} {
		if _, err := resolveSecret("credential", nil, &invalid); err == nil {
			t.Fatal("invalid reference accepted")
		}
	}
	for _, field := range []string{"uri", "username", "password", "api_key"} {
		section := "search:\n    endpoints: [http://search:9200]\n"
		driver := "opensearch"
		if field == "uri" {
			section, driver = "mongodb:\n", "mongodb"
		}
		// Even an explicitly empty literal must not silently lose to a file.
		input := fmt.Sprintf("name: test\nstorage:\n  driver: %s\n  %s    %s: ''\n    %s_file: %q\n", driver, section, field, field, path)
		reader := strings.NewReader(input)
		if _, err := Decode(strings.NewReader("mode: engine"), reader); err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
			t.Fatal("ambiguous credential sources accepted")
		}
	}
}

func TestProjectedCredentialRotationRequiresReload(t *testing.T) {
	directory := t.TempDir()
	first := filepath.Join(directory, "first")
	second := filepath.Join(directory, "second")
	for _, path := range []string{first, second} {
		if err := os.WriteFile(path, []byte(filepath.Base(path)), 0600); err != nil {
			t.Fatal(err)
		}
	}
	link := filepath.Join(directory, "current")
	if err := os.Symlink(first, link); err != nil {
		t.Fatal(err)
	}
	before, err := resolveSecret("credential", nil, &link)
	if err != nil {
		t.Fatal(err)
	}
	replacement := filepath.Join(directory, "replacement")
	if err := os.Symlink(second, replacement); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, link); err != nil {
		t.Fatal(err)
	}
	after, err := resolveSecret("credential", nil, &link)
	if err != nil || before != "first" || after != "second" {
		t.Fatal("projected symlink rotation did not preserve the loaded value until re-read")
	}
}
