<#
On-demand full backup: zips the standalone C:\base project folder and
dumps the local 'analytics' Postgres database, both into the OneDrive-
synced backups folder (which auto-syncs to SharePoint in the background).
Verifies both, logs a combined entry to backup_ledger.jsonl, prunes old sets.
#>
param(
    [string]$Note = "",
    [int]$KeepLast = 5
)

$ErrorActionPreference = "Stop"
$projectDir = "C:\base"
$backupDir  = "C:\Users\mcurphey\OneDrive - A. M. Castle & Co\Documents\backups\full_backups"
$pgBin      = "C:\Program Files\PostgreSQL\15\bin"
$profiles   = "$env:USERPROFILE\.dbt\profiles.yml"
$ledger     = Join-Path $backupDir "backup_ledger.jsonl"

if (-not (Test-Path $projectDir)) { throw "Can't find $projectDir" }
if (-not (Test-Path $backupDir))  { New-Item -ItemType Directory -Path $backupDir -Force | Out-Null }
if (-not (Test-Path $profiles))   { throw "Can't find $profiles - is dbt configured?" }
$profileText = Get-Content $profiles -Raw
if ($profileText -notmatch "pass:\s*(\S+)") { throw "Could not find 'pass:' in $profiles" }
$env:PGPASSWORD = $matches[1]

$stamp   = Get-Date -Format "yyyy-MM-dd_HHmm"
$zipPath = Join-Path $backupDir "base_$stamp.zip"
$dumpPath = Join-Path $backupDir "analytics_$stamp.dump"
$rawLog  = Join-Path $backupDir "dump_log_$stamp.txt"

# --- 1. Zip the project folder ---
Write-Host "==> Zipping $projectDir -> base_$stamp.zip ..." -ForegroundColor Cyan
$zipStart = Get-Date
Compress-Archive -Path (Join-Path $projectDir "*") -DestinationPath $zipPath -Force
$zipDuration = [math]::Round(((Get-Date) - $zipStart).TotalSeconds, 1)

$zipStatus = "FAILED"
$zipSizeMB = $null
$zipEntries = $null
if (Test-Path $zipPath) {
    $zipSizeMB = [math]::Round((Get-Item $zipPath).Length / 1MB, 1)
    Add-Type -AssemblyName System.IO.Compression.FileSystem
    $archive = [System.IO.Compression.ZipFile]::OpenRead($zipPath)
    $zipEntries = $archive.Entries.Count
    $archive.Dispose()
    $zipStatus = if ($zipEntries -gt 0) { "OK" } else { "SUSPECT" }
}

# --- 2. Dump the database ---
Write-Host "==> Backing up 'analytics' -> analytics_$stamp.dump ..." -ForegroundColor Cyan
$dbStart = Get-Date
& "$pgBin\pg_dump.exe" -U postgres -h localhost -Fc -f $dumpPath analytics 2> $rawLog
$dbExitCode = $LASTEXITCODE
$dbDuration = [math]::Round(((Get-Date) - $dbStart).TotalSeconds, 1)

$dbStatus = "FAILED"
$dbSizeMB = $null
$dbObjects = $null
if ($dbExitCode -eq 0 -and (Test-Path $dumpPath)) {
    $dbSizeMB = [math]::Round((Get-Item $dumpPath).Length / 1MB, 1)
    $listOutput = & "$pgBin\pg_restore.exe" --list $dumpPath 2>$null
    $dbObjects = ($listOutput | Select-String -Pattern "^\d+;").Count
    $dbStatus = if ($dbObjects -gt 0) { "OK" } else { "SUSPECT" }
} else {
    Write-Host "pg_dump reported errors -- check dump_log_$stamp.txt" -ForegroundColor Red
}

# --- 3. Log combined entry ---
$entry = [ordered]@{
    timestamp = (Get-Date -Format "o")
    note      = $Note
    project   = [ordered]@{
        file         = "base_$stamp.zip"
        size_mb      = $zipSizeMB
        entries      = $zipEntries
        duration_sec = $zipDuration
        status       = $zipStatus
    }
    database  = [ordered]@{
        file         = "analytics_$stamp.dump"
        size_mb      = $dbSizeMB
        objects      = $dbObjects
        duration_sec = $dbDuration
        status       = $dbStatus
    }
} | ConvertTo-Json -Compress -Depth 5
Add-Content -Path $ledger -Value $entry

# --- Console summary ---
function Color($status) { if ($status -eq "OK") { "Green" } elseif ($status -eq "SUSPECT") { "Yellow" } else { "Red" } }
Write-Host ""
Write-Host "  PROJECT   [$zipStatus]" -ForegroundColor (Color $zipStatus)
Write-Host "    File     : base_$stamp.zip"
Write-Host "    Size     : $zipSizeMB MB, $zipEntries entries"
Write-Host "    Duration : $zipDuration s"
Write-Host ""
Write-Host "  DATABASE  [$dbStatus]" -ForegroundColor (Color $dbStatus)
Write-Host "    File     : analytics_$stamp.dump"
Write-Host "    Size     : $dbSizeMB MB, $dbObjects objects"
Write-Host "    Duration : $dbDuration s"
Write-Host ""
Write-Host "  Ledger: full_backups\backup_ledger.jsonl"
Write-Host ""

# --- 4. Prune old sets (keep newest N by timestamp) ---
$zips = Get-ChildItem $backupDir -Filter "base_*.zip" | Sort-Object LastWriteTime -Descending
if ($zips.Count -gt $KeepLast) {
    foreach ($old in ($zips | Select-Object -Skip $KeepLast)) {
        $oldStamp = $old.BaseName -replace "^base_", ""
        $pair = Get-ChildItem $backupDir -Filter "analytics_$oldStamp.dump" -ErrorAction SilentlyContinue
        Write-Host "Pruning old backup set: $oldStamp" -ForegroundColor DarkGray
        Remove-Item $old.FullName -Force
        if ($pair) { Remove-Item $pair.FullName -Force }
    }
}
