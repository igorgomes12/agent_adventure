"""
API REST para o Intent Agent
=============================
Expõe endpoints para processar prompts e gerar queries.
"""
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from dotenv import load_dotenv
import json

# Carregar .env ANTES de importar o src
load_dotenv(override=True)

from src.factory import IntentAgentFactory
from src.config.settings import Settings

# Criar app FastAPI
app = FastAPI(
    title="Intent Agent API - Processador de Linguagem Natural",
    description="""
    API para processar prompts em linguagem natural e gerar queries estruturadas.
    
    ## 🎯 Endpoint Principal
    
    **POST /query** - Processa um prompt e retorna a query estruturada
    
    ### Exemplo de uso:
    ```json
    {
      "prompt": "liste todos os produtos da categoria Mountain Bikes",
      
    }
    ```
    
    """,
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Em produção, especifique os domínios permitidos
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Variáveis globais
settings = None
agent = None

def get_agent():
    """Inicializa o agente de forma lazy (apenas quando necessário)"""
    global settings, agent
    
    if agent is None:
        settings = Settings.from_env()
        try:
            agent = IntentAgentFactory.create(settings)
            print("✅ Intent Agent inicializado com sucesso")
        except Exception as e:
            print(f"⚠️  Erro ao inicializar agente: {e}")
            print("   O agente será inicializado na primeira requisição")
            raise
    
    return agent


# ── Modelos de Request/Response ──────────────────────────────────

class QueryRequest(BaseModel):
    """Request para processar um prompt"""
    prompt: str = Field(
        ..., 
        description="Prompt em linguagem natural",
        min_length=1
    )
    flow_id: str = Field(
        default="adventureworks_lt",
        description="ID do flow/database"
    )
    include_intent: bool = Field(
        default=False, 
        description="Incluir objeto IntentObject completo na resposta"
    )
    
    model_config = {
        "json_schema_extra": {
            "example": {
                "prompt": "liste todos os produtos da categoria Mountain Bikes"
            }
        }
    }


class QueryResponse(BaseModel):
    """Response com a query estruturada"""
    success: bool = Field(..., description="Se o processamento foi bem-sucedido")
    query_output: Optional[Dict[str, Any]] = Field(None, description="JSON estruturado para o agente de query")
    intent: Optional[Dict[str, Any]] = Field(None, description="Objeto IntentObject completo (se solicitado)")
    error: Optional[str] = Field(None, description="Mensagem de erro (se houver)")
    
    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "success": True,
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
                        "ddl": [
                            {
                                "database": "AdventureWorksLT2022",
                                "tipo": "Sybase",
                                "tables": []
                            }
                        ]
                    },
                    "intent": None,
                    "error": None
                }
            ]
        }
    }


class HealthResponse(BaseModel):
    """Response do health check"""
    status: str
    version: str
    firestore_connected: bool


# ── Endpoints ─────────────────────────────────────────────────────

@app.get("/", response_model=Dict[str, str], include_in_schema=False)
async def root():
    """Endpoint raiz com informações da API"""
    return {
        "message": "Intent Agent API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health"
    }


@app.get("/health", response_model=HealthResponse, include_in_schema=False)
async def health_check():
    """Health check da API"""
    firestore_connected = False
    
    try:
        # Tentar obter o agente
        current_agent = get_agent()
        # Testar conexão com Firestore
        flows = current_agent.repo.get_all_flows()
        firestore_connected = len(flows) >= 0
    except Exception as e:
        print(f"⚠️  Health check: {e}")
        firestore_connected = False
    
    return HealthResponse(
        status="healthy" if firestore_connected else "degraded",
        version="1.0.0",
        firestore_connected=firestore_connected
    )


@app.post("/query", response_model=QueryResponse)
async def process_query(request: QueryRequest):
    """
    Processa um prompt em linguagem natural e retorna a query estruturada.
    
    Este endpoint:
    1. Recebe um prompt em linguagem natural
    2. Identifica a tabela mais relevante
    3. Extrai filtros e colunas
    4. Retorna JSON estruturado para o agente de query
    
    O JSON retornado deve ser enviado para:
    - POST http://localhost:8000/query (Query Generator)
    - POST http://localhost:8001/exec_single_query (Query Executor)
    """
    print(f"\n📥 Recebendo requisição: {request.prompt}")
    
    try:
        # Obter agente (inicializa se necessário)
        print("🔄 Inicializando agente...")
        current_agent = get_agent()
        print("✅ Agente inicializado")
        
        # Processar prompt
        print(f"🤖 Processando prompt com flow_id={request.flow_id}...")
        intent = current_agent.scan_and_process(
            user_prompt=request.prompt,
            flow_id=request.flow_id
        )
        print("✅ Prompt processado")
        
        # Verificar se houve erro
        if intent.status.value == "error":
            return QueryResponse(
                success=False,
                query_output=None,
                intent=intent.to_dict() if request.include_intent else None,
                error=f"Erro ao processar prompt: {intent.warnings[0].message if intent.warnings else 'Erro desconhecido'}"
            )
        
        # Gerar output estruturado
        print("📦 Gerando output estruturado...")
        from src.models.query_output import convert_intent_to_query_format
        query_output = convert_intent_to_query_format(intent, repository=current_agent.repo)
        
        print("✅ Requisição concluída com sucesso")
        return QueryResponse(
            success=True,
            query_output=query_output,
            intent=intent.to_dict() if request.include_intent else None,
            error=None
        )
        
    except Exception as e:
        print(f"❌ Erro ao processar query: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao processar query: {str(e)}"
        )





if __name__ == "__main__":
    import uvicorn
    
    print("\n" + "="*70)
    print("🚀 Iniciando Intent Agent API")
    print("="*70)
    print("\n📖 Documentação interativa: http://localhost:8002/docs")
    print("📊 Health check: http://localhost:8002/health")
    print("\n" + "="*70 + "\n")
    
    uvicorn.run(
        "api:app",
        host="0.0.0.0",
        port=8002,
        reload=True,  # Auto-reload em desenvolvimento
        log_level="info"
    )
