// Package assets embeds the harness's built-in host mechanics and managed guide content. Embedding
// makes the mnemon-harness binary self-contained: setup/render/validate read from FS, never from an
// on-disk source tree.
package assets

import "embed"

//go:embed hosts guides
var FS embed.FS
