// Copyright (c) 2026 Neomantra Corp

package mcpserver

import (
	"fmt"
	"os"

	"github.com/neomantra/CivicSodaQuack/internal/config"
)

// LoadConfigs pairs --db specs with --config paths positionally and loads each
// YAML. Returns a map keyed by spec.Alias. The resulting cfg.DB is overridden
// to the spec's path so writes land in the file the user named (the YAML's
// db: field may be stale — e.g. after a snapshot restore).
//
// Behavior:
//   - len(configPaths) == 0: returns an empty (non-nil) map; no write tools
//     will be available, but the read tools work as before.
//   - len(configPaths) != 0 and != len(specs): error naming both counts.
func LoadConfigs(specs []DBSpec, configPaths []string) (map[string]*config.Config, error) {
	out := make(map[string]*config.Config, len(specs))
	if len(configPaths) == 0 {
		return out, nil
	}
	if len(configPaths) != len(specs) {
		return nil, fmt.Errorf("--db and --config must be paired: got %d --db flags, %d --config flags",
			len(specs), len(configPaths))
	}
	for i, spec := range specs {
		cfg, err := config.Load(configPaths[i])
		if err != nil {
			return nil, fmt.Errorf("--config %s: %w", configPaths[i], err)
		}
		if cfg.DB != "" && cfg.DB != spec.Path {
			fmt.Fprintf(os.Stderr,
				"[csq] warning: --config %s declares db: %q but --db is %q; overriding to %q\n",
				configPaths[i], cfg.DB, spec.Path, spec.Path)
		}
		cfg.DB = spec.Path
		out[spec.Alias] = cfg
	}
	return out, nil
}
