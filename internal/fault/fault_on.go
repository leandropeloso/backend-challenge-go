//go:build faultinject

// Package fault oferece pontos de injeção de falha para os testes de recuperação.
package fault

import "os"

// Hit mata o processo sem executar defers nem shutdown (equivalente a um kill -9)
// quando FAULT_POINT coincide com o ponto informado.
func Hit(point string) {
	if os.Getenv("FAULT_POINT") == point {
		os.Exit(137)
	}
}
