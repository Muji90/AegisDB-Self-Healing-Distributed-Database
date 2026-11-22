# Starts the five-node AegisDB cluster and opens the dashboard.
# Works from any folder and does not need the virtual environment activated first.

$root = $PSScriptRoot
Set-Location $root

$cluster = "node1=http://127.0.0.1:8001,node2=http://127.0.0.1:8002,node3=http://127.0.0.1:8003,node4=http://127.0.0.1:8004,node5=http://127.0.0.1:8005"
$dashboard = "http://127.0.0.1:8001"
$python = Join-Path $root ".venv\Scripts\python.exe"

Write-Host ""
Write-Host "============================================"
Write-Host "          Starting AegisDB Cluster"
Write-Host "============================================"
Write-Host ""

# ---- 1. First run only: create the virtual environment and install packages ----
if (-not (Test-Path $python)) {
    Write-Host "First run: creating virtual environment (.venv) and installing packages..."

    if (Get-Command py -ErrorAction SilentlyContinue) {
        py -3 -m venv .venv
    }
    else {
        python -m venv .venv
    }

    if (-not (Test-Path $python)) {
        Write-Host "Could not create .venv. Install Python 3.10+ from python.org and try again." -ForegroundColor Red
        exit 1
    }

    & $python -m pip install --upgrade pip
    & $python -m pip install -r requirements.txt
}

# ---- 2. Stop any nodes left over from a previous run ----
& (Join-Path $root "stop_cluster.ps1")
Start-Sleep -Seconds 1

# ---- 3. Start the five nodes, each in its own window ----
$nodes = @(
    @{ Id = "node1"; Port = 8001 },
    @{ Id = "node2"; Port = 8002 },
    @{ Id = "node3"; Port = 8003 },
    @{ Id = "node4"; Port = 8004 },
    @{ Id = "node5"; Port = 8005 }
)

foreach ($node in $nodes) {

    $nodeId = $node.Id
    $port = $node.Port

    $command = @"
Set-Location '$root'
`$host.UI.RawUI.WindowTitle = 'AegisDB $nodeId - Port $port'

`$env:NODE_ID='$nodeId'
`$env:NODE_PORT='$port'
`$env:CLUSTER_NODES='$cluster'

`$env:REPLICATION_FACTOR='5'
`$env:READ_QUORUM='3'
`$env:WRITE_QUORUM='3'

`$env:AUTO_RECOVERY_ENABLED='true'
`$env:AUTO_RECOVERY_MIN_SECONDS='25.0'
`$env:AUTO_RECOVERY_MAX_SECONDS='35.0'

Write-Host ''
Write-Host '============================================'
Write-Host 'AegisDB $nodeId - Port $port'
Write-Host 'Replication Factor: 5'
Write-Host 'Read Quorum: 3'
Write-Host 'Write Quorum: 3'
Write-Host 'Automatic Recovery: Enabled'
Write-Host '============================================'
Write-Host ''

& '$python' -m uvicorn aegisdb.main:app --host 127.0.0.1 --port $port
"@

    Start-Process powershell `
        -WorkingDirectory $root `
        -ArgumentList "-NoProfile", "-ExecutionPolicy", "Bypass", "-NoExit", "-Command", $command

    Start-Sleep -Milliseconds 700
}

# ---- 4. Wait until every node answers its health check ----
Write-Host ""
Write-Host "Waiting for all five nodes to come online..."

$deadline = (Get-Date).AddSeconds(60)
$ready = $false

while ((Get-Date) -lt $deadline) {
    $up = 0

    foreach ($node in $nodes) {
        try {
            $response = Invoke-WebRequest `
                -Uri "http://127.0.0.1:$($node.Port)/internal/health" `
                -UseBasicParsing `
                -TimeoutSec 2

            if ($response.StatusCode -eq 200) { $up++ }
        }
        catch { }
    }

    if ($up -eq $nodes.Count) {
        $ready = $true
        break
    }

    Start-Sleep -Seconds 1
}

if (-not $ready) {
    Write-Host "Some nodes did not start within 60 seconds. Check the node windows for errors." -ForegroundColor Yellow
}

# ---- 5. Open the dashboard ----
Write-Host ""
Write-Host "AegisDB cluster started." -ForegroundColor Green
Write-Host "5 nodes | Replication factor 5 | Read quorum 3 | Write quorum 3"
Write-Host "Dashboard:         $dashboard"
Write-Host "API documentation: $dashboard/docs"
Write-Host ""

Start-Process $dashboard
