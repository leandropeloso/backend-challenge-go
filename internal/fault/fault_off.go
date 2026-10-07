//go:build !faultinject

// Package fault oferece pontos de injeção de falha para os testes de recuperação.
// Em builds normais, Hit não faz nada.
package fault

// Hit encerra o processo de forma abrupta quando o ponto está armado. Só
// existe efeito com a build tag faultinject.
func Hit(point string) {}
