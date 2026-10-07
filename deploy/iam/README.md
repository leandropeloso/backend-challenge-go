# Políticas IAM do broker (menor privilégio)

Três identidades, três políticas. Os arquivos `*.policy.json.tmpl` usam placeholders
(`${WAGER_QUEUE_ARN}`, `${DLQ_ARN}`, `${EVENTS_QUEUE_ARN}`) e valem como estão na AWS.

| Identidade | Pode | Não pode |
| --- | --- | --- |
| `gateway-sender` (provedor/gateway que enfileira operações) | `SendMessage` em `wager-transactions.fifo` | ler/apagar mensagens, tocar na DLQ ou nos eventos |
| `wager-app` (esta aplicação) | consumir `wager-transactions.fifo` (receive/delete/change visibility/get attributes); `SendMessage` na DLQ e em `wager-events.fifo` | **enviar** para a fila de entrada, ler os eventos que publica |
| `events-consumer` (quem consome os eventos de saída) | receber/apagar em `wager-events.fifo` | enviar a qualquer fila, tocar na entrada ou na DLQ |

A aplicação **precisa** apenas dessas permissões: o teste
`TestBrokerAccessIsLeastPrivilege` (test/integration) sobe uma instância com as credenciais
de `wager-app` contra um emulador que **impõe** IAM (Moto) e confere que ela consome,
envia à DLQ e publica eventos, e que as outras duas identidades são barradas fora do seu papel.

Em produção, complemente com:

- política de recurso nas filas restringindo `Principal` aos papéis acima;
- criptografia (SSE) e `aws:SecureTransport` nas filas;
- credenciais temporárias (papel IAM da carga de trabalho) em vez de chaves estáticas.
