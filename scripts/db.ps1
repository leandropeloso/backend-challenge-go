# Consulta o banco do ambiente local sem precisar abrir o DBeaver.
#
#   .\scripts\db.ps1                       lista as consultas disponíveis
#   .\scripts\db.ps1 -Consulta carteiras   roda uma consulta de scripts\consultas.sql
#   .\scripts\db.ps1 -Consulta todas       roda todas, em sequência
#   .\scripts\db.ps1 -Sql "select 1"       roda um SQL qualquer (somente leitura)
#
# Precisa do ambiente no ar (docker compose up -d) e usa o psql de dentro do container.
param(
    [string]$Consulta,
    [string]$Sql
)

$ErrorActionPreference = 'Stop'
$env:Path += ";C:\Program Files\Docker\Docker\resources\bin"
Set-Location (Split-Path $PSScriptRoot -Parent)

function Invoke-Psql([string]$text) {
    # BEGIN READ ONLY: mesmo um SQL digitado errado não consegue alterar dados.
    $script = "BEGIN READ ONLY;`n$text`nROLLBACK;"
    $script | docker compose exec -T postgres psql -q -U wager -d wager -P pager=off -v ON_ERROR_STOP=1
}

# Lê o arquivo de consultas e separa cada bloco "-- name: x".
$blocos = [ordered]@{}
$nome = $null
foreach ($linha in Get-Content (Join-Path $PSScriptRoot 'consultas.sql') -Encoding UTF8) {
    if ($linha -match '^-- name:\s*(\w+)') {
        $nome = $Matches[1]
        $blocos[$nome] = New-Object System.Collections.Generic.List[string]
    } elseif ($nome) {
        $blocos[$nome].Add($linha)
    }
}

if ($Sql) {
    Invoke-Psql $Sql
} elseif (-not $Consulta) {
    Write-Host "Consultas disponíveis (use -Consulta <nome>):`n"
    foreach ($k in $blocos.Keys) {
        $desc = ($blocos[$k] | Where-Object { $_ -match '^--' } | Select-Object -First 1) -replace '^--\s*', ''
        '{0,-16} {1}' -f $k, $desc
    }
} elseif ($Consulta -eq 'todas') {
    foreach ($k in $blocos.Keys) {
        Write-Host "`n=== $k ===" -ForegroundColor Cyan
        Invoke-Psql ($blocos[$k] -join "`n")
    }
} elseif ($blocos.Contains($Consulta)) {
    Invoke-Psql ($blocos[$Consulta] -join "`n")
} else {
    throw "Consulta '$Consulta' não existe. Rode .\scripts\db.ps1 para ver a lista."
}
