pipeline {

    agent any

    tools {
        maven 'Maven'
    }

    environment {
        DEPLOY_DIR = 'C:\\apps\\crypto-market-data'

        // Change this if your crypto-market-data application uses another port.
        APP_PORT = '8090'

        APP_JAR = 'crypto-market-data.jar'
        APP_LOG = 'crypto-market-data.log'
        APP_ERROR_LOG = 'crypto-market-data-error.log'
        BACKUP_JAR = 'crypto-market-data-backup.jar'
    }

    stages {

        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Build') {
            steps {
                bat '''
                @echo off

                echo Building Crypto Market Data...

                call mvn clean package -DskipTests

                if errorlevel 1 (
                    echo Maven build failed.
                    exit /b 1
                )

                echo Maven build completed successfully.
                '''
            }
        }

        stage('Test') {
            steps {
                bat '''
                @echo off

                echo Running Crypto Market Data tests...

                call mvn test

                if errorlevel 1 (
                    echo Maven tests failed.
                    exit /b 1
                )

                echo Maven tests completed successfully.
                '''
            }
        }

        stage('Stop old application') {
            steps {
                bat '''
                @echo off
                setlocal EnableDelayedExpansion

                echo Checking for an existing Crypto Market Data process on port %APP_PORT%...

                set "FOUND_PROCESS=false"

                for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":%APP_PORT%" ^| findstr "LISTENING"') do (
                    set "FOUND_PROCESS=true"

                    echo Stopping process PID %%a...

                    taskkill /PID %%a /F >nul 2>&1
                )

                if "!FOUND_PROCESS!"=="false" (
                    echo No existing process was listening on port %APP_PORT%.
                )

                endlocal
                exit /b 0
                '''
            }
        }

        stage('Wait for port release') {
            steps {
                powershell '''
                    Write-Host "Waiting for port $env:APP_PORT to be released..."

                    $maxAttempts = 12
                    $released = $false

                    for ($attempt = 1; $attempt -le $maxAttempts; $attempt++) {

                        $listener = Get-NetTCPConnection `
                            -LocalPort ([int]$env:APP_PORT) `
                            -State Listen `
                            -ErrorAction SilentlyContinue

                        if (-not $listener) {

                            Write-Host "Port $env:APP_PORT is available."

                            $released = $true
                            break
                        }

                        Write-Host "Port is still in use. Attempt $attempt of $maxAttempts..."

                        Start-Sleep -Seconds 2
                    }

                    if (-not $released) {

                        Write-Error "Port $env:APP_PORT could not be released."

                        exit 1
                    }
                '''
            }
        }

        stage('Prepare deployment folder') {
            steps {
                bat '''
                @echo off

                if not exist "%DEPLOY_DIR%" (

                    echo Creating deployment directory...

                    mkdir "%DEPLOY_DIR%"
                )

                if errorlevel 1 (

                    echo Failed to create deployment directory.

                    exit /b 1
                )

                if exist "%DEPLOY_DIR%\\%APP_JAR%" (

                    echo Creating backup of the current JAR...

                    copy /Y ^
                        "%DEPLOY_DIR%\\%APP_JAR%" ^
                        "%DEPLOY_DIR%\\%BACKUP_JAR%" >nul

                    if errorlevel 1 (

                        echo Failed to create backup.

                        exit /b 1
                    )

                    echo Backup created successfully.

                ) else (

                    echo No existing JAR was found. Backup skipped.
                )
                '''
            }
        }

        stage('Deploy new JAR') {
            steps {
                bat '''
                @echo off
                setlocal EnableDelayedExpansion

                set "FOUND_JAR="

                for %%f in (target\\*.jar) do (
                    set "FOUND_JAR=%%f"
                )

                if not defined FOUND_JAR (

                    echo No executable JAR was found in the target folder.

                    exit /b 1
                )

                echo Deploying !FOUND_JAR!...

                copy /Y ^
                    "!FOUND_JAR!" ^
                    "%DEPLOY_DIR%\\%APP_JAR%" >nul

                if errorlevel 1 (

                    echo Failed to copy the application JAR.

                    exit /b 1
                )

                echo New JAR deployed successfully.

                endlocal
                '''
            }
        }

        stage('Start application') {
            steps {
                bat '''
                @echo off

                cd /d "%DEPLOY_DIR%"

                if exist "%APP_LOG%" (
                    del /Q "%APP_LOG%"
                )

                if exist "%APP_ERROR_LOG%" (
                    del /Q "%APP_ERROR_LOG%"
                )

                echo Starting Crypto Market Data on port %APP_PORT%...

                set JENKINS_NODE_COOKIE=crypto-market-data-dont-kill

                start "Crypto Market Data" /B cmd /c ^
                    javaw -jar "%APP_JAR%" ^
                    --server.port=%APP_PORT% ^
                    1^> "%APP_LOG%" ^
                    2^> "%APP_ERROR_LOG%"

                echo Startup command executed.
                '''
            }
        }

        stage('Verify port') {
            steps {
                powershell '''
                    Write-Host "Checking port $env:APP_PORT..."

                    $maxAttempts = 18
                    $delaySeconds = 5
                    $listening = $false

                    for ($attempt = 1; $attempt -le $maxAttempts; $attempt++) {

                        $listener = Get-NetTCPConnection `
                            -LocalPort ([int]$env:APP_PORT) `
                            -State Listen `
                            -ErrorAction SilentlyContinue

                        if ($listener) {

                            Write-Host "Crypto Market Data is listening on port $env:APP_PORT."
                            Write-Host "PID: $($listener.OwningProcess)"

                            $listening = $true
                            break
                        }

                        Write-Host "Port check attempt $attempt of $maxAttempts failed."

                        Start-Sleep -Seconds $delaySeconds
                    }

                    if (-not $listening) {

                        Write-Host ""
                        Write-Host "Crypto Market Data did not start successfully."
                        Write-Host ""

                        Write-Host "Application output:"
                        Write-Host "----------------------------------------"

                        $logPath = Join-Path $env:DEPLOY_DIR $env:APP_LOG
                        $errorLogPath = Join-Path $env:DEPLOY_DIR $env:APP_ERROR_LOG

                        if (Test-Path $logPath) {

                            Get-Content $logPath

                        } else {

                            Write-Host "Output log not found: $logPath"
                        }

                        Write-Host ""
                        Write-Host "Application errors:"
                        Write-Host "----------------------------------------"

                        if (Test-Path $errorLogPath) {

                            Get-Content $errorLogPath

                        } else {

                            Write-Host "Error log not found: $errorLogPath"
                        }

                        Write-Host "----------------------------------------"

                        exit 1
                    }
                '''
            }
        }

        stage('Confirm process remains running') {
            steps {
                powershell '''
                    Write-Host "Confirming that Crypto Market Data remains running..."

                    Start-Sleep -Seconds 10

                    $listener = Get-NetTCPConnection `
                        -LocalPort ([int]$env:APP_PORT) `
                        -State Listen `
                        -ErrorAction SilentlyContinue

                    if (-not $listener) {

                        Write-Host "Crypto Market Data stopped after deployment."

                        $logPath = Join-Path $env:DEPLOY_DIR $env:APP_LOG
                        $errorLogPath = Join-Path $env:DEPLOY_DIR $env:APP_ERROR_LOG

                        Write-Host ""
                        Write-Host "Application output:"

                        if (Test-Path $logPath) {
                            Get-Content $logPath
                        }

                        Write-Host ""
                        Write-Host "Application errors:"

                        if (Test-Path $errorLogPath) {
                            Get-Content $errorLogPath
                        }

                        exit 1
                    }

                    Write-Host "Crypto Market Data is still running."
                    Write-Host "PID: $($listener.OwningProcess)"
                '''
            }
        }

    }

    post {

        success {

            echo 'Crypto Market Data was built, tested, deployed, started, and verified successfully.'

            echo "Local URL: http://localhost:${APP_PORT}"

            echo "External URL: http://169.58.108.119:${APP_PORT}"

            echo "Application log: ${DEPLOY_DIR}\\${APP_LOG}"

            echo "Error log: ${DEPLOY_DIR}\\${APP_ERROR_LOG}"
        }

        failure {

            echo 'Crypto Market Data deployment failed. Attempting rollback...'

            powershell '''

                $deployDir = $env:DEPLOY_DIR

                $appJar = Join-Path $deployDir $env:APP_JAR
                $backupJar = Join-Path $deployDir $env:BACKUP_JAR

                $logPath = Join-Path $deployDir $env:APP_LOG
                $errorLogPath = Join-Path $deployDir $env:APP_ERROR_LOG

                $listeners = Get-NetTCPConnection `
                    -LocalPort ([int]$env:APP_PORT) `
                    -State Listen `
                    -ErrorAction SilentlyContinue

                foreach ($listener in $listeners) {

                    Write-Host "Stopping failed deployment process PID $($listener.OwningProcess)..."

                    Stop-Process `
                        -Id $listener.OwningProcess `
                        -Force `
                        -ErrorAction SilentlyContinue
                }

                Start-Sleep -Seconds 3

                if (-not (Test-Path $backupJar)) {

                    Write-Host "No backup JAR exists. Automatic rollback is unavailable."

                    exit 0
                }

                Write-Host "Restoring backup JAR..."

                Copy-Item `
                    -Path $backupJar `
                    -Destination $appJar `
                    -Force

                if (Test-Path $logPath) {
                    Remove-Item $logPath -Force
                }

                if (Test-Path $errorLogPath) {
                    Remove-Item $errorLogPath -Force
                }

                Write-Host "Starting restored application..."

                $javaArguments = @(
                    "-jar"
                    "`"$appJar`""
                    "--server.port=$env:APP_PORT"
                )

                Start-Process `
                    -FilePath "javaw.exe" `
                    -ArgumentList $javaArguments `
                    -WorkingDirectory $deployDir `
                    -RedirectStandardOutput $logPath `
                    -RedirectStandardError $errorLogPath

                Start-Sleep -Seconds 35

                $restoredListener = Get-NetTCPConnection `
                    -LocalPort ([int]$env:APP_PORT) `
                    -State Listen `
                    -ErrorAction SilentlyContinue

                if ($restoredListener) {

                    Write-Host "Rollback succeeded."

                    Write-Host "Restored Crypto Market Data is listening on port $env:APP_PORT."

                    Write-Host "PID: $($restoredListener.OwningProcess)"

                } else {

                    Write-Host "Rollback failed."

                    Write-Host ""
                    Write-Host "Application output:"

                    if (Test-Path $logPath) {
                        Get-Content $logPath
                    }

                    Write-Host ""
                    Write-Host "Application errors:"

                    if (Test-Path $errorLogPath) {
                        Get-Content $errorLogPath
                    }

                    exit 1
                }

            '''

            echo 'Check Jenkins Console Output.'

            echo "Application log: ${DEPLOY_DIR}\\${APP_LOG}"

            echo "Error log: ${DEPLOY_DIR}\\${APP_ERROR_LOG}"
        }

        always {

            archiveArtifacts(
                artifacts: 'target/*.jar',
                fingerprint: true,
                allowEmptyArchive: true
            )
        }
    }
}