// Copyright (c) 2026 Neomantra Corp

// Package version exposes the build-time-injected csq version string.
//
// Override at build time:
//
//	go build -ldflags "-X github.com/neomantra/CivicSodaQuack/internal/version.Version=<value>" ...
//
// The Taskfile's `build` task injects `git describe --tags --always --dirty`
// (falling back to the literal default when not in a git checkout).
package version

// Version is the csq build version. Defaults to a -dev literal; replaced at
// build time via -ldflags for releases and tagged builds.
var Version = "0.6.0-dev"
