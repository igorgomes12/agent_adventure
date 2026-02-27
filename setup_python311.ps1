# Setup Python 3.11.3
Write-Host "Configurando Python 3.11.3..." -ForegroundColor Green

# Verificar se Python 3.11 esta instalado
$version = py -3.11 --version 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host "Encontrado: $version" -ForegroundColor Green
} else {
    Write-Host "Python 3.11.3 nao encontrado" -ForegroundColor Red
    Write-Host "Baixe em: https://www.python.org/downloads/release/python-3113/" -ForegroundColor Yellow
    exit 1
}

# Criar ambiente virtual
Write-Host "Criando ambiente virtual venv311..." -ForegroundColor Green
py -3.11 -m venv venv311

# Ativar ambiente virtual
Write-Host "Ativando ambiente virtual..." -ForegroundColor Green
& .\venv311\Scripts\Activate.ps1

# Atualizar pip
Write-Host "Atualizando pip..." -ForegroundColor Green
& .\venv311\Scripts\python.exe -m pip install --upgrade pip

# Instalar dependencias
Write-Host "Instalando dependencias..." -ForegroundColor Green
& .\venv311\Scripts\pip.exe install -r requirements-api.txt

Write-Host ""
Write-Host "Setup concluido!" -ForegroundColor Green
Write-Host ""
Write-Host "Para iniciar a API:" -ForegroundColor Cyan
Write-Host "   .\start_api.ps1" -ForegroundColor White
Write-Host ""
