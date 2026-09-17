package config

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"
)

const maxSecretFileBytes = 1 << 20

// resolveSecret reads a mounted credential once. It never interprets its bytes
// as YAML or expands environment variables, and never includes them in errors.
func resolveSecret(field string, value *string, path *string) (string, error) {
	if path == nil {
		if value == nil {
			return "", nil
		}
		return strings.TrimSpace(*value), nil
	}
	if value != nil {
		return "", fmt.Errorf("%s and %s_file are mutually exclusive", field, field)
	}
	if !filepath.IsAbs(*path) {
		return "", fmt.Errorf("%s_file must be an absolute path", field)
	}
	// Follow Kubernetes projected-volume symlinks, but reject directories/devices
	// and FIFOs before opening them (a FIFO could otherwise block startup).
	info, err := os.Stat(*path)
	if err != nil || !info.Mode().IsRegular() {
		return "", fmt.Errorf("%s_file must reference a readable regular file", field)
	}
	file, err := os.Open(*path)
	if err != nil {
		return "", fmt.Errorf("%s_file cannot be opened", field)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxSecretFileBytes+1))
	if err != nil {
		return "", fmt.Errorf("%s_file cannot be read", field)
	}
	if len(data) == 0 || len(data) > maxSecretFileBytes || !utf8.Valid(data) || strings.ContainsRune(string(data), '\x00') {
		return "", fmt.Errorf("%s_file must contain 1 byte to 1 MiB of UTF-8 text without NUL", field)
	}
	// Exact bytes matter for passwords; operators must avoid accidental newlines.
	return string(data), nil
}
