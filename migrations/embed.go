// Package migrations embute os scripts SQL versionados.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
