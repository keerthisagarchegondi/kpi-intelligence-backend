# KPI Intelligence Backend - PowerShell Management Scripts
# Production-level automation for Windows environments

param(
    [Parameter(Position=0)]
    [string]$Command = "help"
)

# Colors for output
function Write-Success {
    param([string]$Message)
    Write-Host "✓ $Message" -ForegroundColor Green
}

function Write-Info {
    param([string]$Message)
    Write-Host "ℹ $Message" -ForegroundColor Cyan
}

function Write-Warning {
    param([string]$Message)
    Write-Host "⚠ $Message" -ForegroundColor Yellow
}

function Write-Error {
    param([string]$Message)
    Write-Host "✗ $Message" -ForegroundColor Red
}

function Write-Header {
    param([string]$Message)
    Write-Host "`n$('=' * 80)" -ForegroundColor Magenta
    Write-Host $Message.PadLeft(($Message.Length + 80) / 2) -ForegroundColor Magenta
    Write-Host "$('=' * 80)`n" -ForegroundColor Magenta
}

# Command implementations
function Invoke-DockerBuild {
    Write-Header "Building Docker Images"
    docker-compose build --no-cache
    Write-Success "Docker images built successfully"
}

function Invoke-DockerUp {
    Write-Header "Starting Services"
    docker-compose up -d
    Write-Success "Services started successfully"
    Start-Sleep -Seconds 5
    Invoke-HealthCheck
}

function Invoke-DockerDown {
    Write-Header "Stopping Services"
    docker-compose down
    Write-Success "Services stopped successfully"
}

function Invoke-DockerLogs {
    Write-Header "Viewing Logs (Press Ctrl+C to exit)"
    docker-compose logs -f
}

function Invoke-ProductionDeploy {
    Write-Header "Production Deployment"
    
    Write-Info "Stopping existing services..."
    docker-compose down
    
    Write-Info "Building fresh images..."
    docker-compose build --no-cache
    
    Write-Info "Starting production services..."
    docker-compose up -d
    
    Write-Info "Waiting for services to be healthy..."
    Start-Sleep -Seconds 10
    
    Invoke-HealthCheck
    Write-Success "Production deployment complete!"
}

function Invoke-TestLocal {
    Write-Header "Running Local Integration Tests"
    python run_integration_tests.py
}

function Invoke-TestDocker {
    Write-Header "Running Integration Tests in Docker"
    docker-compose --profile test up test
}

function Invoke-TestAll {
    Write-Header "Running All Integration Tests"
    Invoke-TestLocal
}

function Invoke-HealthCheck {
    Write-Header "Health Check"
    
    Write-Info "Checking API health..."
    try {
        $response = Invoke-RestMethod -Uri "http://localhost:8000/health" -Method Get -TimeoutSec 5
        Write-Success "API is healthy: $($response.status)"
    } catch {
        Write-Error "API is not responding"
    }
    
    Write-Info "Checking database..."
    try {
        $dbCheck = docker-compose exec -T db pg_isready -U kpiuser -d kpi_intelligence
        Write-Success "Database is ready"
    } catch {
        Write-Error "Database is not ready"
    }
}

function Invoke-Clean {
    Write-Header "Cleaning Up"
    
    Write-Info "Stopping and removing containers..."
    docker-compose down -v
    
    Write-Info "Removing Python cache..."
    Get-ChildItem -Path . -Include __pycache__,*.pyc -Recurse -Force | Remove-Item -Force -Recurse
    
    Write-Info "Cleaning data directories..."
    if (Test-Path "data\processed") {
        Get-ChildItem "data\processed\*" | Remove-Item -Force
    }
    
    if (Test-Path "logs") {
        Get-ChildItem "logs\*" | Remove-Item -Force
    }
    
    if (Test-Path "test_reports") {
        Get-ChildItem "test_reports\*" | Remove-Item -Force
    }
    
    Write-Success "Cleanup complete"
}

function Invoke-DBShell {
    Write-Header "Opening Database Shell"
    docker-compose exec db psql -U kpiuser -d kpi_intelligence
}

function Invoke-DBBackup {
    $timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $backupFile = "backup_$timestamp.sql"
    
    Write-Header "Database Backup"
    Write-Info "Creating backup: $backupFile"
    
    docker-compose exec -T db pg_dump -U kpiuser kpi_intelligence > $backupFile
    
    if (Test-Path $backupFile) {
        Write-Success "Backup created successfully: $backupFile"
    } else {
        Write-Error "Backup failed"
    }
}

function Invoke-DevRun {
    Write-Header "Starting Development Server"
    python -m uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
}

function Show-Help {
    Write-Header "KPI Intelligence Backend - Available Commands"
    
    Write-Host "Production Commands:" -ForegroundColor Yellow
    Write-Host "  .\manage.ps1 production-deploy  - Build and deploy production environment"
    Write-Host "  .\manage.ps1 docker-build        - Build Docker images"
    Write-Host "  .\manage.ps1 docker-up           - Start all services"
    Write-Host "  .\manage.ps1 docker-down         - Stop all services"
    Write-Host "  .\manage.ps1 docker-logs         - View logs"
    Write-Host ""
    
    Write-Host "Testing Commands:" -ForegroundColor Yellow
    Write-Host "  .\manage.ps1 test-local          - Run integration tests locally"
    Write-Host "  .\manage.ps1 test-docker         - Run integration tests in Docker"
    Write-Host "  .\manage.ps1 test-all            - Run all tests"
    Write-Host ""
    
    Write-Host "Database Commands:" -ForegroundColor Yellow
    Write-Host "  .\manage.ps1 db-shell            - Open database shell"
    Write-Host "  .\manage.ps1 db-backup           - Backup database"
    Write-Host ""
    
    Write-Host "Monitoring Commands:" -ForegroundColor Yellow
    Write-Host "  .\manage.ps1 health-check        - Check service health"
    Write-Host ""
    
    Write-Host "Development Commands:" -ForegroundColor Yellow
    Write-Host "  .\manage.ps1 dev-run             - Start development server"
    Write-Host ""
    
    Write-Host "Cleanup Commands:" -ForegroundColor Yellow
    Write-Host "  .\manage.ps1 clean               - Clean up containers and temp files"
    Write-Host ""
}

# Main command router
switch ($Command) {
    "docker-build" { Invoke-DockerBuild }
    "docker-up" { Invoke-DockerUp }
    "docker-down" { Invoke-DockerDown }
    "docker-logs" { Invoke-DockerLogs }
    "production-deploy" { Invoke-ProductionDeploy }
    "test-local" { Invoke-TestLocal }
    "test-docker" { Invoke-TestDocker }
    "test-all" { Invoke-TestAll }
    "health-check" { Invoke-HealthCheck }
    "clean" { Invoke-Clean }
    "db-shell" { Invoke-DBShell }
    "db-backup" { Invoke-DBBackup }
    "dev-run" { Invoke-DevRun }
    "help" { Show-Help }
    default {
        Write-Error "Unknown command: $Command"
        Show-Help
    }
}
