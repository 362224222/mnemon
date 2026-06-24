// Package assets embeds the harness's built-in host mechanics. Embedding
// makes the mnemon-harness binary self-contained: setup/render/validate read from FS, never from an
// on-disk source tree.
package assets

import "embed"

//go:embed hosts
var FS embed.FS
