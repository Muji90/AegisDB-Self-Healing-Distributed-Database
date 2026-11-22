Write-Host "Stopping AegisDB nodes..."

$ports = @(
    8001,
    8002,
    8003,
    8004,
    8005
)

foreach ($port in $ports) {

    $connections = Get-NetTCPConnection `
        -LocalPort $port `
        -ErrorAction SilentlyContinue

    foreach ($connection in $connections) {

        $processId = `
            $connection.OwningProcess

        if ($processId) {

            Stop-Process `
                -Id $processId `
                -Force `
                -ErrorAction SilentlyContinue
        }
    }
}

Write-Host "AegisDB cluster stopped."