# Teste ponta a ponta contra o ambiente do `docker compose up --build` (3 instâncias).
# Percorre HTTP, SQS, outbox/eventos, isolamento entre provedores, traces e desligamento gracioso.
# Uso:  pwsh scripts/e2e.ps1        (precisa do Docker CLI no PATH)
param(
    [string[]] $Bases = @('http://localhost:8081', 'http://localhost:8082', 'http://localhost:8083'),
    [string] $Keycloak = 'http://localhost:8080',
    [string] $Jaeger = 'http://localhost:16686'
)
$ErrorActionPreference = 'Stop'
$script:pass = 0; $script:fail = 0
function Check($name, [bool] $ok, $detail = '') {
    if ($ok) { $script:pass++; Write-Host "  ok   $name" } else { $script:fail++; Write-Host "  FAIL $name  $detail" -ForegroundColor Red }
}
function Token($client, $secret) {
    (Invoke-RestMethod -Method Post -Uri "$Keycloak/realms/wager/protocol/openid-connect/token" `
        -Body @{ grant_type = 'client_credentials'; client_id = $client; client_secret = $secret }).access_token
}
function Call($method, $url, $token, $body = $null, $headers = @{}) {
    $h = @{} + $headers
    if ($token) { $h['Authorization'] = "Bearer $token" }
    $p = @{ Method = $method; Uri = $url; Headers = $h; SkipHttpErrorCheck = $true; ContentType = 'application/json' }
    if ($null -ne $body) { $p['Body'] = ($body | ConvertTo-Json -Depth 8) }
    $r = Invoke-WebRequest @p
    $json = $null
    if ($r.Content) { try { $json = $r.Content | ConvertFrom-Json } catch {} }
    [pscustomobject]@{ Status = [int]$r.StatusCode; Json = $json; Headers = $r.Headers }
}
function Wait-Until($what, [scriptblock] $cond, $seconds = 40) {
    $end = (Get-Date).AddSeconds($seconds)
    while ((Get-Date) -lt $end) { if (& $cond) { return $true }; Start-Sleep -Milliseconds 400 }
    return $false
}
function Money($a) { @{ amount = $a; currency = 'BRL' } }
$b1, $b2, $b3 = $Bases

Write-Host '== identidades (Keycloak)'
$internal = Token 'internal-service' 'internal-service-secret'
$provA = Token 'provider-a' 'provider-a-secret'
$provB = Token 'provider-b' 'provider-b-secret'
Check 'tokens obtidos' ($internal -and $provA -and $provB)

Write-Host '== carteira'
$player = [guid]::NewGuid().ToString()
$w = Call 'POST' "$b1/wallets" $internal @{ playerId = $player; initialBalance = (Money '1000.00') }
Check 'abre carteira (201, saldo 1000.00, versão 1)' ($w.Status -eq 201 -and $w.Json.balance.amount -eq '1000.00' -and $w.Json.version -eq 1)
$wid = $w.Json.id
Check 'segunda carteira do mesmo jogador/moeda: 409' ((Call 'POST' "$b2/wallets" $internal @{ playerId = $player; initialBalance = (Money '1.00') }).Status -eq 409)

function Op($kind, $amount, $ext, $ref = $null, $round = 'round-1') {
    $o = @{ providerId = 'provider-a'; externalTransactionId = $ext; playerId = $player; walletId = $wid; roundId = $round
            gameId = 'fortune-chimp'; kind = $kind; money = (Money $amount) }
    if ($ref) { $o['referenceExternalTransactionId'] = $ref }
    $o
}
$run = [guid]::NewGuid().ToString().Substring(0, 8)

Write-Host '== operações (cada uma por uma instância diferente) e idempotência'
$bet = Op 'BET' '25.00' "bet-$run"
$r1 = Call 'POST' "$b2/wagering/transactions" $provA $bet @{ 'Idempotency-Key' = "provider-a:bet-$run" }
Check 'BET 25.00 processada, saldo 975.00' ($r1.Status -eq 200 -and $r1.Json.status -eq 'PROCESSED' -and $r1.Json.balance.amount -eq '975.00' -and -not $r1.Json.idempotentReplay)
$r2 = Call 'POST' "$b3/wagering/transactions" $provA $bet @{ 'Idempotency-Key' = "provider-a:bet-$run" }
Check 'replay por outra instância: mesma transação, idempotentReplay=true' ($r2.Status -eq 200 -and $r2.Json.idempotentReplay -and $r2.Json.transactionId -eq $r1.Json.transactionId)
$changed = Op 'BET' '30.00' "bet-$run"
Check 'mesma chave com conteúdo diferente: 409' ((Call 'POST' "$b1/wagering/transactions" $provA $changed @{ 'Idempotency-Key' = "provider-a:bet-$run" }).Status -eq 409)
Check 'mesma operação externa com outra chave: 409' ((Call 'POST' "$b1/wagering/transactions" $provA $bet @{ 'Idempotency-Key' = 'outra-chave' }).Status -eq 409)
Check 'sem Idempotency-Key: 400' ((Call 'POST' "$b1/wagering/transactions" $provA $bet).Status -eq 400)
Check 'valor com uma casa decimal: 400' ((Call 'POST' "$b1/wagering/transactions" $provA (Op 'BET' '5.5' "x-$run") @{ 'Idempotency-Key' = "x-$run" }).Status -eq 400)

$win = Call 'POST' "$b3/wagering/transactions" $provA (Op 'WIN' '10.00' "win-$run") @{ 'Idempotency-Key' = "win-$run" }
Check 'WIN 10.00: saldo 985.00' ($win.Json.balance.amount -eq '985.00')
$loss = Call 'POST' "$b1/wagering/transactions" $provA (Op 'LOSS' '0.00' "loss-$run") @{ 'Idempotency-Key' = "loss-$run" }
Check 'LOSS 0.00: processada, saldo inalterado' ($loss.Status -eq 200 -and $loss.Json.balance.amount -eq '985.00')
$refund = Call 'POST' "$b2/wagering/transactions" $provA (Op 'REFUND' '25.00' "refund-$run" "bet-$run") @{ 'Idempotency-Key' = "refund-$run" }
Check 'REFUND integral da aposta: saldo 1010.00' ($refund.Status -eq 200 -and $refund.Json.balance.amount -eq '1010.00')
$again = Call 'POST' "$b3/wagering/transactions" $provA (Op 'ROLLBACK' '25.00' "rb-$run" "bet-$run") @{ 'Idempotency-Key' = "rb-$run" }
Check 'ROLLBACK da mesma aposta (já reembolsada): 422 ALREADY_REVERSED' ($again.Status -eq 422 -and $again.Json.failureCode -eq 'ALREADY_REVERSED')
$broke = Call 'POST' "$b1/wagering/transactions" $provA (Op 'BET' '999999.00' "broke-$run") @{ 'Idempotency-Key' = "broke-$run" }
Check 'saldo insuficiente: 422 INSUFFICIENT_FUNDS' ($broke.Status -eq 422 -and $broke.Json.failureCode -eq 'INSUFFICIENT_FUNDS')

Write-Host '== referência que chega depois'
$early = Call 'POST' "$b1/wagering/transactions" $provA (Op 'REFUND' '12.00' "early-refund-$run" "late-bet-$run") @{ 'Idempotency-Key' = "early-refund-$run" }
Check 'REFUND antes da aposta: 202 PENDING_REFERENCE' ($early.Status -eq 202 -and $early.Json.status -eq 'PENDING_REFERENCE')
$null = Call 'POST' "$b2/wagering/transactions" $provA (Op 'BET' '12.00' "late-bet-$run") @{ 'Idempotency-Key' = "late-bet-$run" }
$resolved = Wait-Until 'refund pendente resolvido' {
    (Call 'GET' "$b3/providers/provider-a/wagering/transactions/early-refund-$run" $provA).Json.status -eq 'PROCESSED' }
Check 'a pendência é resolvida sozinha depois que a aposta chega' $resolved

Write-Host '== leituras, ledger, reconciliação e partidas dobradas'
$wal = Call 'GET' "$b2/wallets/$wid" $internal
Check 'saldo final 1010.00 (1000 − 25 + 10 + 25 − 12 + 12), versão coerente' ($wal.Json.balance.amount -eq '1010.00')
$page1 = Call 'GET' "$b1/wallets/$wid/ledger?limit=2" $internal
Check 'ledger: página de 2 com cursor' ($page1.Json.items.Count -eq 2 -and $page1.Json.nextCursor)
$seen = @($page1.Json.items.walletVersion); $cur = $page1.Json.nextCursor
while ($cur) { $pg = Call 'GET' "$b3/wallets/$wid/ledger?limit=2&cursor=$cur" $internal; $seen += @($pg.Json.items.walletVersion); $cur = $pg.Json.nextCursor }
Check 'ledger: versões 1..N sem lacunas nem repetições' (($seen -join ',') -eq ((1..$seen.Count) -join ','))
$rec = Call 'POST' "$b2/wallets/$wid/reconciliation" $internal
Check 'reconciliação consistente (diferença 0.00)' ($rec.Json.consistent -and $rec.Json.difference.amount -eq '0.00' -and $rec.Json.checkedEntries -eq $seen.Count)
$tb = Call 'GET' "$b1/accounting/trial-balance" $internal
Check 'balancete do diário equilibrado (débitos = créditos)' ($tb.Status -eq 200 -and $tb.Json.balanced)

Write-Host '== autenticação, autorização e isolamento'
Check 'sem token: 401' ((Call 'GET' "$b1/wallets/$wid" $null).Status -eq 401)
Check 'token lixo: 401' ((Call 'GET' "$b1/wallets/$wid" 'lixo').Status -eq 401)
Check 'provedor em rota de carteira: 403' ((Call 'GET' "$b1/wallets/$wid" $provA).Status -eq 403)
Check 'serviço interno não envia operações: 403' ((Call 'POST' "$b1/wagering/transactions" $internal (Op 'BET' '1.00' "i-$run") @{ 'Idempotency-Key' = "i-$run" }).Status -eq 403)
$txid = $r1.Json.transactionId
Check 'provedor B não lê a transação de A (404)' ((Call 'GET' "$b2/wagering/transactions/$txid" $provB).Status -eq 404)
Check 'provedor B não lê pela rota de A (403)' ((Call 'GET' "$b2/providers/provider-a/wagering/transactions/bet-$run" $provB).Status -eq 403)
Check 'provedor B não se passa por A (403)' ((Call 'POST' "$b3/wagering/transactions" $provB $bet @{ 'Idempotency-Key' = "provider-a:bet-$run" }).Status -eq 403)
Check 'dono e serviço interno leem a transação' ((Call 'GET' "$b2/wagering/transactions/$txid" $provA).Status -eq 200 -and (Call 'GET' "$b2/wagering/transactions/$txid" $internal).Status -eq 200)

Write-Host '== SQS (entrada) e DLQ'
$msgId = "msg-$run"
$sqsBody = @{ messageId = $msgId; type = 'WagerTransactionRequested'; occurredAt = (Get-Date).ToUniversalTime().ToString('o')
              data = (Op 'BET' '5.00' "sqs-bet-$run") }
$sqsBody.data['idempotencyKey'] = "provider-a:sqs-bet-$run"
$file = Join-Path ([IO.Path]::GetTempPath()) "e2e-$run.json"
[IO.File]::WriteAllText($file, ($sqsBody | ConvertTo-Json -Depth 8 -Compress))
docker cp $file wager-localstack-1:/tmp/e2e.json | Out-Null
docker compose exec -T localstack awslocal sqs send-message --queue-url http://localhost:4566/000000000000/wager-transactions.fifo `
    --message-body file:///tmp/e2e.json --message-group-id $wid --message-deduplication-id $msgId | Out-Null
$viaSqs = Wait-Until 'operação SQS processada' { (Call 'GET' "$b1/providers/provider-a/wagering/transactions/sqs-bet-$run" $provA).Json.status -eq 'PROCESSED' }
Check 'mensagem SQS processada (BET 5.00)' $viaSqs
$replaySqs = Call 'POST' "$b2/wagering/transactions" $provA (Op 'BET' '5.00' "sqs-bet-$run") @{ 'Idempotency-Key' = "provider-a:sqs-bet-$run" }
Check 'a mesma operação por HTTP depois do SQS é replay' ($replaySqs.Json.idempotentReplay -and $replaySqs.Json.balance.amount -eq '1005.00')
docker compose exec -T localstack awslocal sqs send-message --queue-url http://localhost:4566/000000000000/wager-transactions.fifo `
    --message-body 'isto nao e json' --message-group-id $wid --message-deduplication-id "bad-$run" | Out-Null
$dlq = Wait-Until 'mensagem inválida na DLQ' {
    [int]((docker compose exec -T localstack awslocal sqs get-queue-attributes --queue-url http://localhost:4566/000000000000/wager-transactions-dlq.fifo `
        --attribute-names ApproximateNumberOfMessages --query Attributes.ApproximateNumberOfMessages --output text).Trim()) -ge 1 }
Check 'mensagem inválida vai para a DLQ' $dlq

Write-Host '== eventos de saída (outbox -> SQS)'
$types = @{}
$got = Wait-Until 'eventos publicados' {
    $raw = docker compose exec -T localstack awslocal sqs receive-message --queue-url http://localhost:4566/000000000000/wager-events.fifo `
        --max-number-of-messages 10 --wait-time-seconds 1 --visibility-timeout 1 --output json
    # consome (apaga) o que lê: numa FIFO, mensagens em voo seguram as seguintes do mesmo grupo
    if ($raw) { foreach ($m in (($raw | ConvertFrom-Json).Messages)) {
        $e = $m.Body | ConvertFrom-Json; $types[$e.eventType] = $true
        docker compose exec -T localstack awslocal sqs delete-message --queue-url http://localhost:4566/000000000000/wager-events.fifo `
            --receipt-handle $m.ReceiptHandle | Out-Null } }
    $types.ContainsKey('WagerTransactionProcessed') -and $types.ContainsKey('WalletBalanceChanged') -and $types.ContainsKey('WagerTransactionRejected') -and $types.ContainsKey('WagerTransactionPendingReference') }
Check 'os 4 tipos de evento chegaram à fila de saída' $got ($types.Keys -join ',')

Write-Host '== traces (OpenTelemetry/Jaeger)'
$traceId = -join ((1..32) | ForEach-Object { '{0:x}' -f (Get-Random -Maximum 16) })
$span = -join ((1..16) | ForEach-Object { '{0:x}' -f (Get-Random -Maximum 16) })
$null = Call 'POST' "$b2/wagering/transactions" $provA (Op 'BET' '1.00' "trace-$run") @{ 'Idempotency-Key' = "trace-$run"; traceparent = "00-$traceId-$span-01" }
$names = @()
$traced = Wait-Until 'trace no Jaeger' {
    try { $t = Invoke-RestMethod "$Jaeger/api/traces/$traceId"; $script:names = @($t.data[0].spans.operationName)
          ($script:names -contains 'wager.execute') -and ($script:names -contains 'db.transaction') -and ($script:names -contains 'outbox.publish') } catch { $false } } 45
Check 'o trace do cliente chega a wager.execute, db.transaction e outbox.publish' $traced ($names -join ',')

Write-Host '== métricas'
$m = (Invoke-WebRequest "$b1/metrics").Content
Check 'métricas de resultados, duplicatas, outbox e latência expostas' (($m -match 'wager_transactions_total') -and ($m -match 'wager_outbox_published_total') -and ($m -match 'wager_processing_duration_seconds'))
$mAll = ($Bases | ForEach-Object { (Invoke-WebRequest "$_/metrics").Content }) -join "`n"
Check 'duplicatas contabilizadas' ($mAll -match 'wager_duplicates_total\{source="http"\} [1-9]')

Write-Host '== desligamento gracioso e recuperação'
docker compose stop app3 | Out-Null
$exit = (docker inspect wager-app3-1 --format '{{.State.ExitCode}}').Trim()
Check 'SIGTERM: a instância encerra com código 0' ($exit -eq '0') "exit=$exit"
$still = Call 'POST' "$b2/wagering/transactions" $provA (Op 'BET' '1.00' "after-stop-$run") @{ 'Idempotency-Key' = "after-stop-$run" }
Check 'as outras instâncias seguem atendendo' ($still.Status -eq 200)
docker compose start app3 | Out-Null
$back = Wait-Until 'app3 de volta' { try { (Invoke-RestMethod "$b3/health/ready").status -eq 'ok' } catch { $false } } 60
Check 'a instância volta e fica pronta' $back
$final = Call 'POST' "$b3/wallets/$wid/reconciliation" $internal
Check 'reconciliação final consistente' ($final.Json.consistent)

Write-Host ''
Write-Host "RESULTADO: $script:pass verificações ok, $script:fail falhas"
if ($script:fail -gt 0) { exit 1 }
