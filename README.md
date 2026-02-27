# Intent Agent API - AdventureWorks

API REST para processar prompts em linguagem natural e gerar queries estruturadas para o banco de dados AdventureWorks.

## 🚀 Início Rápido

### Requisitos

- Python 3.11.3
- Firestore configurado
- Google AI Studio API Key

### Instalação

1. Clone o repositório
2. Configure o ambiente virtual:

```powershell
.\setup_python311.ps1
```

3. Configure as variáveis de ambiente no arquivo `.env`:

```env
# Google Cloud Platform
GCP_PROJECT_ID=seu-projeto
GCP_LOCATION=us-central1
GOOGLE_APPLICATION_CREDENTIALS=./key.json

# Gemini API
USE_VERTEX_AI=false
GOOGLE_API_KEY=sua-api-key
GEMINI_THRESHOLD=0.5

# Firestore
USE_FIRESTORE=true
FIRESTORE_PROJECT_ID=seu-projeto
FIRESTORE_CREDENTIALS=./firestore-key.json
FIRESTORE_DATABASE=(default)
```

### Iniciar a API

```powershell
.\start_api.ps1
```

A API estará disponível em:
- 📖 Swagger: http://localhost:8002/docs
- 🏥 Health: http://localhost:8002/health

## 📡 Endpoints

### POST /query

Processa um prompt em linguagem natural e retorna a query estruturada.

**Request:**
```json
{
  "prompt": "liste todos os produtos da categoria Mountain Bikes"
}
```

**Response:**
```json
{
  "success": true,
  "query_output": {
    "parameters": {
      "filter_fields": [
        {"AdventureWorksLT2022.SalesLT.ProductCategory.Name": "= 'Mountain Bikes'"}
      ],
      "tables": [
        "AdventureWorksLT2022.SalesLT.Product",
        "AdventureWorksLT2022.SalesLT.ProductCategory"
      ],
      "return_fields": [
        "AdventureWorksLT2022.SalesLT.Product.ProductID",
        "AdventureWorksLT2022.SalesLT.Product.Name"
      ]
    },
    "ddl": [...]
  },
  "intent": null,
  "error": null
}
```

### GET /health

Verifica o status da API e conexão com Firestore.

**Response:**
```json
{
  "status": "healthy",
  "version": "1.0.0",
  "firestore_connected": true
}
```

### 🤖 Vertex AI Agent Integration

#### POST /vertex-agent/tool

Endpoint para o Vertex AI Agent chamar ferramentas localmente (Function Calling).

**Request:**
```json
{
  "tool_name": "buscar_catalogo_tabelas",
  "parameters": {
    "flow_id": "adventureworks_lt"
  }
}
```

**Response:**
```json
{
  "success": true,
  "result": {
    "tables": [...],
    "count": 10
  }
}
```

#### GET /vertex-agent/tools

Retorna as definições de ferramentas para configurar no Vertex AI Agent Builder.

**Ferramentas disponíveis:**
- `buscar_catalogo_tabelas`: Busca todas as tabelas de um flow
- `validar_e_formatar_intent`: Valida e formata intent completo

**Veja o guia completo:** [VERTEX_AGENT_SETUP.md](VERTEX_AGENT_SETUP.md)

## 🔧 Estrutura do Projeto

```
intent_agent/
├── api.py                          # API FastAPI
├── src/
│   ├── agent/                      # Agente de processamento
│   ├── config/                     # Configurações
│   ├── models/                     # Modelos de dados
│   ├── repositories/               # Repositórios (Firestore)
│   ├── services/                   # Serviços (AI, validação)
│   └── strategies/                 # Estratégias de processamento
├── tests/                          # Testes unitários
├── .env                            # Variáveis de ambiente
├── setup_python311.ps1             # Script de setup
└── start_api.ps1                   # Script para iniciar API
```

## 🧪 Testes

Execute os testes unitários:

```powershell
.\venv311\Scripts\python.exe -m pytest tests/ -v
```

Com coverage:

```powershell
.\venv311\Scripts\python.exe -m pytest tests/ --cov=src --cov-report=html
```

## 📝 Scripts Utilitários

### adicionar_tabelas_firestore.py
Adiciona tabelas ao Firestore a partir de definições JSON.

### configurar_database_name.py
Configura o nome do database no Firestore.

### exemplo_frontend.html
Exemplo de frontend para testar a API.

## 🔗 Integração

O JSON retornado pela API deve ser enviado para:

1. **Query Generator** (porta 8000):
   ```
   POST http://localhost:8000/query
   ```

2. **Query Executor** (porta 8001):
   ```
   POST http://localhost:8001/exec_single_query
   ```

## 📦 Dependências Principais

- fastapi==0.115.6
- uvicorn==0.34.0
- pydantic==2.10.5
- google-cloud-firestore
- google-genai
- google-cloud-aiplatform

## 🐛 Solução de Problemas

### Erro: "ModuleNotFoundError"
Certifique-se de que o ambiente virtual está ativado e as dependências instaladas:
```powershell
.\venv311\Scripts\Activate.ps1
pip install -r requirements-api.txt
```

### Erro: "503 UNAVAILABLE" (Gemini)
A API do Gemini está com alta demanda. Aguarde alguns minutos e tente novamente.

### Erro: Firestore não conecta
Verifique se o arquivo de credenciais está correto e se o projeto tem permissões adequadas.

## 📄 Licença

Este projeto é proprietário.
