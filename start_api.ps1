# Iniciar API com Python 3.11.3

# Verificar se ambiente virtual existe
if (-not (Test-Path ".\venv311\Scripts\Activate.ps1")) {
    Write-Host "Ambiente virtual nao encontrado!" -ForegroundColor Red
    Write-Host "Execute primeiro: .\setup_python311.ps1" -ForegroundColor Yellow
    exit 1
}

# Ativar ambiente virtual
Write-Host "Ativando ambiente virtual Python 3.11.3..." -ForegroundColor Green
& .\venv311\Scripts\Activate.ps1

# Verificar versao
Write-Host "Python Version:" -ForegroundColor Green
& .\venv311\Scripts\python.exe --version

Write-Host ""

# Iniciar API
Write-Host "Iniciando API na porta 8002..." -ForegroundColor Green
Write-Host "Documentacao: http://localhost:8002/docs" -ForegroundColor Cyan
Write-Host ""

& .\venv311\Scripts\python.exe api.py
