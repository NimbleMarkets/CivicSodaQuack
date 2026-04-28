// Copyright (c) 2026 Neomantra Corp

package version

import "testing"

func TestVersion_DefaultIsDev(t *testing.T) {
	// When tests run without -ldflags, the package var should hold its default.
	// Production builds override this via the Taskfile.
	if Version == "" {
		t.Errorf("Version should never be empty")
	}
	// We don't pin the exact default here; if it changes, that's a one-line
	// codebase update. The point is to guard against accidental empty defaults.
}
