// Copyright (c) 2026 Neomantra Corp

package sync

import "os"

func writeStderr(p []byte) (int, error) {
	return os.Stderr.Write(p)
}
