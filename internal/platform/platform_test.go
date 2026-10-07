package platform

import (
	"testing"

	"go.uber.org/fx"
)

// O grafo de dependências precisa estar completo e sem ciclos; isso é checado
// sem abrir conexões nem iniciar workers.
func TestGraphIsValid(t *testing.T) {
	if err := fx.ValidateApp(Options()); err != nil {
		t.Fatalf("grafo Fx inválido: %v", err)
	}
}
