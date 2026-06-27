$VERSION = if ($env:VERSION) { $env:VERSION } else { "1.0.0" }
$COMMIT = git rev-parse --short HEAD 2>$null
if (-not $COMMIT) { $COMMIT = "dev" }
$BUILD_TIME = Get-Date -Format "yyyy-MM-ddTHH:mm:ssZ"
$LDFLAGS = "-s -w -X main.Version=$VERSION -X main.Commit=$COMMIT -X main.BuildTime=$BUILD_TIME"

Write-Host "Building AFD v$VERSION ($COMMIT)" -ForegroundColor Cyan

C:\Go\bin\go.exe build -ldflags $LDFLAGS -trimpath -o bin/afd.exe ./cmd/afd

if ($LASTEXITCODE -eq 0) {
    $size = [math]::Round((Get-Item "bin\afd.exe").Length / 1MB, 2)
    Write-Host "Build successful! Size: ${size} MB" -ForegroundColor Green
} else {
    Write-Host "Build failed!" -ForegroundColor Red
    exit 1
}
