//go:build nofrontend

package main

import "embed"

// frontendEmbedFS is an empty FS used when building without the frontend
// (e.g. the worker-only Docker image).  The master UI is not served in
// this configuration.
var frontendEmbedFS embed.FS
