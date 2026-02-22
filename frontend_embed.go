//go:build !nofrontend

package main

import "embed"

//go:embed all:web/out
var frontendEmbedFS embed.FS
