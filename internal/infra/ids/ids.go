// Package ids gera identificadores UUIDv7 (ordenáveis por tempo).
package ids

import "github.com/google/uuid"

type Generator struct{}

// New devolve um UUIDv7; se o gerador de entropia falhar, cai para v4 em vez de abortar.
func (Generator) New() uuid.UUID {
	if id, err := uuid.NewV7(); err == nil {
		return id
	}
	return uuid.New()
}
