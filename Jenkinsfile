pipeline {
    agent any
    tools { maven 'Maven' }
    environment {
        DEPLOY_DIR = 'C:\\apps\\crypto-market-data'
        APP_PORT = '8090'
        APP_JAR = 'crypto-market-data.jar'
        BACKUP_JAR = 'crypto-market-data-backup.jar'
    }
    options {
        disableConcurrentBuilds()
        buildDiscarder(logRotator(numToKeepStr: '10', artifactNumToKeepStr: '10'))
    }
    stages {
        stage('Checkout') { steps { checkout scm } }
        stage('Build') { steps { bat 'call mvn clean package -DskipTests' } }
        stage('Test') { steps { bat 'call mvn test' } }
        stage('Deploy through WinSW') {
            steps {
                powershell '''
                    $ErrorActionPreference = 'Stop'
                    $serviceName = 'CryptoMarketData'
                    $jar = Join-Path $env:DEPLOY_DIR $env:APP_JAR
                    $backup = Join-Path $env:DEPLOY_DIR $env:BACKUP_JAR
                    $serviceDir = Join-Path $env:DEPLOY_DIR 'service'
                    $wrapper = Join-Path $serviceDir 'CryptoMarketDataService.exe'
                    $xmlPath = Join-Path $serviceDir 'CryptoMarketDataService.xml'
                    $stopScript = Join-Path $serviceDir 'jenkins-stop.ps1'
                    $startScript = Join-Path $serviceDir 'jenkins-start.ps1'
                    $serviceKey = "HKLM:/SYSTEM/CurrentControlSet/Services/$serviceName"
                    $port = [int]$env:APP_PORT

                    function Service { Get-CimInstance Win32_Service -Filter "Name='$serviceName'" }
                    function Snapshot {
                        [pscustomobject]@{
                            Service = (Service)
                            Processes = @(Get-CimInstance Win32_Process)
                            Owners = @(Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue |
                                Select-Object -ExpandProperty OwningProcess -Unique)
                        }
                    }
                    function Collectors($snapshot) {
                        @($snapshot.Processes | Where-Object {
                            $_.Name -match '^java(w)?[.]exe$' -and
                            $_.CommandLine -match '(?i)crypto-market-data[.]jar'
                        })
                    }
                    function Assert-Owned($snapshot) {
                        $apps = @(Collectors $snapshot)
                        if ($snapshot.Service.State -ne 'Running' -or $snapshot.Service.ProcessId -eq 0) {
                            throw 'Collector service is not running.'
                        }
                        if ($apps.Count -ne 1 -or $snapshot.Owners.Count -ne 1) {
                            throw 'Expected exactly one collector JVM and one listener owner.'
                        }
                        $app = $apps[0]
                        if ($app.ParentProcessId -ne $snapshot.Service.ProcessId -or
                            $app.ProcessId -ne $snapshot.Owners[0] -or
                            $app.ExecutablePath -ine $java -or
                            $app.CommandLine -notmatch [regex]::Escape($jar)) {
                            throw 'Foreign collector or listener; manual ownership recovery required.'
                        }
                        $parent = @($snapshot.Processes | Where-Object ProcessId -eq $snapshot.Service.ProcessId)
                        if ($parent.Count -ne 1 -or $parent[0].ExecutablePath -ine $wrapper -or
                            $parent[0].CreationDate -gt $app.CreationDate) {
                            throw 'WinSW process ancestry is invalid.'
                        }
                    }
                    function Assert-Stopped($snapshot) {
                        if ($snapshot.Service.State -ne 'Stopped' -or $snapshot.Service.StartMode -ne 'Disabled' -or
                            @(Collectors $snapshot).Count -ne 0 -or $snapshot.Owners.Count -ne 0) {
                            throw 'Stopped/disabled service, absent collector JVM and free port are required.'
                        }
                    }
                    function Assert-Hash($path, $expected) {
                        if ((Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash -ne $expected) {
                            throw "Artifact checksum mismatch: $path"
                        }
                    }
                    function Assert-ApprovedRelease($path) {
                        $approved = '856F605363557623AF8C54BB18D4D06C84152318074CA4368F0BAF8DB755C5A0'
                        if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
                            throw 'Approved release artifact is missing.'
                        }
                        Assert-Hash $path $approved
                    }
                    function Invoke-ServiceScript($path) {
                        & powershell.exe -NoProfile -NonInteractive -File $path
                        if ($LASTEXITCODE -ne 0) { throw "Service script failed: $path" }
                    }
                    function Fence-And-Stop {
                        Set-Service -Name $serviceName -StartupType Disabled
                        Invoke-ServiceScript $stopScript
                        Assert-Stopped (Snapshot)
                    }
                    function Restore-Startup {
                        $mode = if ($wasDelayed) { 'delayed-auto' } else { 'auto' }
                        & sc.exe config $serviceName start= $mode
                        if ($LASTEXITCODE -ne 0) { throw 'Cannot restore service startup mode.' }
                    }
                    function Assert-Healthy($hash) {
                        Assert-Owned (Snapshot)
                        Assert-Hash $jar $hash
                        $health = Invoke-RestMethod -Uri "http://localhost:$port/actuator/health/readiness" -TimeoutSec 5
                        if ($health.status -ne 'UP') { throw 'Collector ingestion readiness is not UP.' }
                        Assert-Owned (Snapshot)
                    }

                    # No mutation until prerequisites and existing ownership pass.
                    foreach ($path in @($jar, $wrapper, $xmlPath, $stopScript, $startScript)) {
                        if (-not (Test-Path -LiteralPath $path -PathType Leaf)) { throw "Missing prerequisite: $path" }
                    }
                    [xml]$config = Get-Content -LiteralPath $xmlPath -Raw
                    $java = [Environment]::ExpandEnvironmentVariables([string]$config.service.executable)
                    if ([string]$config.service.workingdirectory -ine $env:DEPLOY_DIR -or
                        [string]$config.service.arguments -notmatch [regex]::Escape($jar)) {
                        throw 'Installed WinSW application path contract differs.'
                    }
                    $initial = Snapshot
                    if ($initial.Service.PathName.Trim('"') -ine $wrapper -or $initial.Service.StartMode -ne 'Auto') {
                        throw 'Unexpected service installation or startup mode.'
                    }
                    Assert-Owned $initial
                    $wasDelayed = (Get-ItemProperty -LiteralPath $serviceKey -Name DelayedAutoStart -ErrorAction SilentlyContinue).DelayedAutoStart -eq 1
                    $candidates = @(Get-ChildItem -LiteralPath 'target' -Filter '*.jar' -File)
                    if ($candidates.Count -ne 1) { throw 'Expected exactly one built executable JAR.' }
                    $candidate = $candidates[0].FullName
                    Assert-ApprovedRelease $candidate
                    Add-Type -AssemblyName System.IO.Compression.FileSystem
                    $archive = [IO.Compression.ZipFile]::OpenRead($candidate)
                    try {
                        if (-not $archive.GetEntry('BOOT-INF/classes/com/crypto/marketdata/CryptoMarketDataApplication.class')) {
                            throw 'Candidate is not the executable collector artifact.'
                        }
                    } finally { $archive.Dispose() }
                    $newHash = (Get-FileHash -LiteralPath $candidate -Algorithm SHA256).Hash
                    $oldHash = (Get-FileHash -LiteralPath $jar -Algorithm SHA256).Hash
                    $xmlHash = (Get-FileHash -LiteralPath $xmlPath -Algorithm SHA256).Hash
                    $recoveryBefore = @( & sc.exe qfailure $serviceName ) -join "`n"
                    if ($LASTEXITCODE -ne 0) { throw 'Cannot record service recovery settings.' }
                    Write-Host "CandidateSHA256=$newHash PreviousSHA256=$oldHash"
                    $fenced = $false
                    try {
                        Assert-Owned (Snapshot)
                        Assert-ApprovedRelease $candidate
                        # Set before the mutation so any partial fencing failure is retained for review.
                        $fenced = $true
                        Fence-And-Stop
                        Assert-Hash $jar $oldHash
                        $stamp = (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssfffZ')
                        $immutableBackup = "$backup.$stamp"
                        if (Test-Path -LiteralPath $immutableBackup) { throw 'Backup already exists.' }
                        Copy-Item -LiteralPath $jar -Destination $immutableBackup
                        Assert-Hash $immutableBackup $oldHash
                        Copy-Item -LiteralPath $jar -Destination $backup -Force
                        Assert-Hash $backup $oldHash
                        Assert-Stopped (Snapshot)
                        Assert-Hash $candidate $newHash
                        Copy-Item -LiteralPath $candidate -Destination $jar -Force
                        Assert-Hash $jar $newHash
                        Assert-Hash $xmlPath $xmlHash
                        Restore-Startup
                        Invoke-ServiceScript $startScript
                        Assert-Healthy $newHash
                        Start-Sleep -Seconds 10
                        Assert-Healthy $newHash
                        $recoveryAfter = @( & sc.exe qfailure $serviceName ) -join "`n"
                        if ($LASTEXITCODE -ne 0 -or $recoveryAfter -cne $recoveryBefore) {
                            throw 'Automatic service recovery settings changed.'
                        }
                        Write-Host "WinSW deployment verified; SHA256=$newHash; rollback=$immutableBackup"
                    } catch {
                        if ($fenced) {
                            # Never replace a JAR or kill a port owner during failure recovery.
                            # Disable retry startup, request SCM stop only, and report if drain fails.
                            try { Fence-And-Stop } catch { Write-Warning "Stop/fence unconfirmed: $($_.Exception.Message)" }
                            Write-Warning 'RECOVERY_REQUIRED: service fenced; retain artifacts/logs; separately approve rollback.'
                        }
                        throw
                    }
                '''
            }
        }
    }
    post {
        success { echo 'Collector built, tested and verified under WinSW ownership.' }
        failure { echo 'Deployment failed. Inspect exact service/artifact state; no external Java rollback is permitted.' }
        always { archiveArtifacts(artifacts: 'target/*.jar', fingerprint: true, allowEmptyArchive: true) }
    }
}
