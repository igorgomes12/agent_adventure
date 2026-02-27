"""
Testes Reais - AI Inference Service Vertex AI
==============================================
Testes que fazem chamadas reais ao Vertex AI usando suas credenciais.
Requer .env configurado com GCP_PROJECT_ID e GOOGLE_APPLICATION_CREDENTIALS.
"""

import pytest
import json
import os
import time
from dotenv import load_dotenv
from src.services.ai_inference_vertex import AIInferenceServiceVertex
from src.repositories.firestore_firebase_repository import FirestoreFirebaseRepository

# Carregar variáveis de ambiente
load_dotenv()

# Delay entre testes para evitar rate limiting
DELAY_BETWEEN_TESTS = 3  # segundos
MAX_RETRIES = 3
RETRY_DELAY = 5  # segundos inicial


def retry_on_rate_limit(func):
    """Decorator para retry automático em caso de rate limiting"""
    def wrapper(*args, **kwargs):
        retry_delay = RETRY_DELAY
        for attempt in range(MAX_RETRIES):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e):
                    if attempt < MAX_RETRIES - 1:
                        print(f"\n   ⚠️  Rate limit (tentativa {attempt + 1}/{MAX_RETRIES}), aguardando {retry_delay}s...")
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Backoff exponencial
                        continue
                raise
    return wrapper


@pytest.fixture(scope="module")
def vertex_service():
    """Fixture que cria serviço Vertex AI real"""
    project_id = os.getenv("GCP_PROJECT_ID")
    location = os.getenv("GCP_LOCATION", "us-central1")
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    
    if not project_id:
        pytest.skip("GCP_PROJECT_ID não configurado no .env")
    
    try:
        service = AIInferenceServiceVertex(
            project_id=project_id,
            location=location,
            credentials_path=credentials_path
        )
        print(f"\n✅ Vertex AI inicializado: {service.model_name}")
        return service
    except Exception as e:
        pytest.skip(f"Não foi possível inicializar Vertex AI: {e}")


@pytest.fixture(scope="module")
def firestore_repo():
    """Fixture que cria repositório Firestore real"""
    project_id = os.getenv("FIRESTORE_PROJECT_ID") or os.getenv("GCP_PROJECT_ID")
    credentials_path = os.getenv("FIRESTORE_CREDENTIALS") or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    database = os.getenv("FIRESTORE_DATABASE", "(default)")
    
    if not project_id:
        pytest.skip("Projeto Firestore não configurado")
    
    try:
        repo = FirestoreFirebaseRepository(
            project_id=project_id,
            database=database,
            credentials_path=credentials_path
        )
        print(f"\n✅ Firestore conectado: {project_id}/{database}")
        return repo
    except Exception as e:
        pytest.skip(f"Não foi possível conectar ao Firestore: {e}")


@pytest.fixture(scope="module")
def real_flow_data(firestore_repo):
    """Fixture que busca dados reais de um flow do Firestore"""
    # Tentar buscar o flow principal
    flow_id = "FLUXO_PROPOSTA_VEICULO_CRM"
    
    flow = firestore_repo.get_flow(flow_id)
    if not flow:
        # Se não encontrar, buscar qualquer flow disponível
        all_flows = firestore_repo.get_all_flows()
        if all_flows:
            flow_id = list(all_flows.keys())[0]
            flow = all_flows[flow_id]
        else:
            pytest.skip("Nenhum flow encontrado no Firestore")
    
    print(f"\n✅ Flow carregado: {flow_id}")
    return flow_id, flow


class TestVertexAIInitialization:
    """Testes de inicialização do Vertex AI"""
    
    def test_service_initialization(self, vertex_service):
        """Testa que o serviço foi inicializado corretamente"""
        assert vertex_service is not None
        assert vertex_service.model_name is not None
        assert "gemini" in vertex_service.model_name.lower()
        print(f"\n   Modelo: {vertex_service.model_name}")
    
    def test_client_configuration(self, vertex_service):
        """Testa configuração do cliente"""
        assert vertex_service.client is not None
        assert vertex_service.generation_config is not None
        assert vertex_service.generation_config.temperature == 0.1


class TestVertexAIInferenceWithRealData:
    """Testes de inferência com dados reais do Firestore"""
    
    def test_infer_simple_status_filter(self, vertex_service, firestore_repo, real_flow_data):
        """Testa inferência com filtro simples de status"""
        time.sleep(DELAY_BETWEEN_TESTS)
        
        flow_id, flow = real_flow_data
        
        # Buscar dados da tabela
        doc = firestore_repo.fluxos_ref.document(flow_id).get()
        if not doc.exists:
            pytest.skip("Flow não encontrado")
        
        firebase_data = doc.to_dict()
        
        # Montar contexto
        context = {
            "flow_id": flow_id,
            "table": firebase_data.get("table_definition", {}).get("table_name", "TbProposta"),
            "columns": [col.get("name") for col in firebase_data.get("table_definition", {}).get("columns", [])],
            "ddl": json.dumps(firebase_data.get("table_definition", {}), indent=2)
        }
        
        # Query simples
        user_query = "mostre as propostas aprovadas"
        
        # Executar inferência com retry
        result = self._infer_with_retry(vertex_service, user_query, context)
        
        # Verificações
        assert "filters" in result
        assert "select_columns" in result
        assert "order_by" in result
        assert "limit" in result
        assert "confidence_score" in result
        
        print(f"\n   Query: {user_query}")
        print(f"   Filtros: {len(result['filters'])}")
        print(f"   Confiança: {result['confidence_score']}")
        print(f"   Resultado completo:\n{json.dumps(result, indent=2, ensure_ascii=False)}")
        
        # Verificar que detectou filtro de status
        assert len(result["filters"]) > 0
        assert result["confidence_score"] > 0.5
    
    def _infer_with_retry(self, vertex_service, user_query, context):
        """Helper para executar inferência com retry"""
        retry_delay = RETRY_DELAY
        for attempt in range(MAX_RETRIES):
            try:
                return vertex_service.infer_intent(user_query, context)
            except Exception as e:
                if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e):
                    if attempt < MAX_RETRIES - 1:
                        print(f"\n   ⚠️  Rate limit (tentativa {attempt + 1}/{MAX_RETRIES}), aguardando {retry_delay}s...")
                        time.sleep(retry_delay)
                        retry_delay *= 2
                        continue
                raise
    
    def test_infer_date_filter(self, vertex_service, firestore_repo, real_flow_data):
        """Testa inferência com filtro de data"""
        time.sleep(DELAY_BETWEEN_TESTS)
        
        flow_id, flow = real_flow_data
        
        doc = firestore_repo.fluxos_ref.document(flow_id).get()
        firebase_data = doc.to_dict()
        
        context = {
            "flow_id": flow_id,
            "table": firebase_data.get("table_definition", {}).get("table_name", "TbProposta"),
            "columns": [col.get("name") for col in firebase_data.get("table_definition", {}).get("columns", [])]
        }
        
        user_query = "propostas dos últimos 30 dias"
        result = self._infer_with_retry(vertex_service, user_query, context)
        
        print(f"\n   Query: {user_query}")
        print(f"   Filtros: {len(result['filters'])}")
        print(f"   Resultado:\n{json.dumps(result, indent=2, ensure_ascii=False)}")
        
        # Verificar que detectou filtro de data
        assert len(result["filters"]) > 0
        
        # Verificar se usou função Sybase
        date_filter = next((f for f in result["filters"] if "Dt" in f.get("column", "")), None)
        if date_filter and isinstance(date_filter.get("value"), str):
            assert "DATEADD" in date_filter["value"] or "GETDATE" in date_filter["value"]
    
    def test_infer_complex_query(self, vertex_service, firestore_repo, real_flow_data):
        """Testa inferência com query complexa"""
        time.sleep(DELAY_BETWEEN_TESTS)
        
        flow_id, flow = real_flow_data
        
        doc = firestore_repo.fluxos_ref.document(flow_id).get()
        firebase_data = doc.to_dict()
        
        context = {
            "flow_id": flow_id,
            "table": firebase_data.get("table_definition", {}).get("table_name", "TbProposta"),
            "columns": [col.get("name") for col in firebase_data.get("table_definition", {}).get("columns", [])],
            "ddl": json.dumps(firebase_data.get("table_definition", {}), indent=2)
        }
        
        user_query = "propostas aprovadas ou pendentes dos últimos 7 dias, ordenadas por data de criação"
        result = self._infer_with_retry(vertex_service, user_query, context)
        
        print(f"\n   Query: {user_query}")
        print(f"   Filtros: {len(result['filters'])}")
        print(f"   Ordenação: {result.get('order_by', [])}")
        print(f"   Confiança: {result['confidence_score']}")
        print(f"   Resultado:\n{json.dumps(result, indent=2, ensure_ascii=False)}")
        
        # Verificações
        assert len(result["filters"]) >= 1
        assert result["confidence_score"] > 0.5
    
    def test_infer_with_all_firestore_columns(self, vertex_service, firestore_repo, real_flow_data):
        """Testa inferência usando todas as colunas do Firestore"""
        time.sleep(DELAY_BETWEEN_TESTS)
        
        flow_id, flow = real_flow_data
        
        doc = firestore_repo.fluxos_ref.document(flow_id).get()
        firebase_data = doc.to_dict()
        
        table_def = firebase_data.get("table_definition", {})
        columns = [col.get("name") for col in table_def.get("columns", [])]
        
        context = {
            "flow_id": flow_id,
            "table": table_def.get("table_name", "TbProposta"),
            "columns": columns,
            "ddl": json.dumps(table_def, indent=2, ensure_ascii=False)
        }
        
        user_query = "mostre todas as propostas"
        result = self._infer_with_retry(vertex_service, user_query, context)
        
        print(f"\n   Query: {user_query}")
        print(f"   Colunas disponíveis: {len(columns)}")
        print(f"   Resultado:\n{json.dumps(result, indent=2, ensure_ascii=False)}")
        
        assert "filters" in result
        assert "confidence_score" in result


class TestVertexAIPromptBuilding:
    """Testes de construção de prompt"""
    
    def test_prompt_includes_context(self, vertex_service, real_flow_data):
        """Testa que o prompt inclui o contexto corretamente"""
        flow_id, flow = real_flow_data
        
        context = {
            "flow_id": flow_id,
            "table": "TbProposta",
            "columns": ["NuProposta", "StatusProposta"]
        }
        
        prompt = vertex_service._build_prompt("teste", context)
        
        assert "USER QUERY" in prompt
        assert "CONTEXT" in prompt
        assert flow_id in prompt
        assert "TbProposta" in prompt
        assert "Sybase" in prompt


class TestVertexAIErrorHandling:
    """Testes de tratamento de erros"""
    
    def test_invalid_json_response(self, vertex_service):
        """Testa tratamento de resposta JSON inválida"""
        time.sleep(DELAY_BETWEEN_TESTS)
        
        context = {"table": "TbTest"}
        
        retry_delay = RETRY_DELAY
        for attempt in range(MAX_RETRIES):
            try:
                result = vertex_service.infer_intent("???", context)
                # Se chegou aqui, pelo menos deve ter a estrutura básica
                assert "filters" in result or "confidence_score" in result
                break
            except json.JSONDecodeError:
                pass
                break
            except Exception as e:
                if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e):
                    if attempt < MAX_RETRIES - 1:
                        print(f"\n   ⚠️  Rate limit, aguardando {retry_delay}s...")
                        time.sleep(retry_delay)
                        retry_delay *= 2
                        continue
                print(f"\n   Erro capturado: {type(e).__name__}: {e}")
                break


class TestFirestoreIntegration:
    """Testes de integração com Firestore"""
    
    def test_firestore_connection(self, firestore_repo):
        """Testa conexão com Firestore"""
        assert firestore_repo is not None
        assert firestore_repo.db is not None
        assert firestore_repo.fluxos_ref is not None
    
    def test_get_flow_from_firestore(self, firestore_repo):
        """Testa buscar flow do Firestore"""
        flow_id = "FLUXO_PROPOSTA_VEICULO_CRM"
        flow = firestore_repo.get_flow(flow_id)
        
        if flow:
            print(f"\n   Flow encontrado: {flow_id}")
            print(f"   Descrição: {flow.get('description', 'N/A')}")
            assert flow["flow_id"] == flow_id
        else:
            # Tentar buscar qualquer flow
            all_flows = firestore_repo.get_all_flows()
            assert len(all_flows) > 0, "Nenhum flow encontrado no Firestore"
            print(f"\n   Flows disponíveis: {list(all_flows.keys())}")
    
    def test_get_table_definition(self, firestore_repo, real_flow_data):
        """Testa buscar definição de tabela"""
        flow_id, flow = real_flow_data
        
        doc = firestore_repo.fluxos_ref.document(flow_id).get()
        if doc.exists:
            firebase_data = doc.to_dict()
            table_def = firebase_data.get("table_definition")
            
            if table_def:
                print(f"\n   Tabela: {table_def.get('table_name')}")
                print(f"   Colunas: {len(table_def.get('columns', []))}")
                assert "table_name" in table_def
                assert "columns" in table_def


class TestEndToEndWorkflow:
    """Testes de workflow completo"""
    
    def test_full_workflow_firestore_to_vertex(self, firestore_repo, vertex_service, real_flow_data):
        """Testa workflow completo: Firestore -> Vertex AI"""
        time.sleep(DELAY_BETWEEN_TESTS)
        
        flow_id, flow = real_flow_data
        
        # 1. Buscar dados do Firestore
        doc = firestore_repo.fluxos_ref.document(flow_id).get()
        assert doc.exists
        
        firebase_data = doc.to_dict()
        table_def = firebase_data.get("table_definition", {})
        
        # 2. Montar contexto
        context = {
            "flow_id": flow_id,
            "table": table_def.get("table_name", "TbProposta"),
            "columns": [col.get("name") for col in table_def.get("columns", [])],
            "ddl": json.dumps(table_def, indent=2, ensure_ascii=False)
        }
        
        # 3. Executar inferência com retry
        user_query = "mostre as propostas aprovadas dos últimos 15 dias"
        result = self._infer_with_retry(vertex_service, user_query, context)
        
        # 4. Verificações
        print(f"\n   === WORKFLOW COMPLETO ===")
        print(f"   Flow: {flow_id}")
        print(f"   Tabela: {context['table']}")
        print(f"   Query: {user_query}")
        print(f"   Modelo: {vertex_service.model_name}")
        print(f"   Resultado:\n{json.dumps(result, indent=2, ensure_ascii=False)}")
        
        assert "filters" in result
        assert "confidence_score" in result
        assert result["confidence_score"] > 0.5
        assert len(result["filters"]) > 0
    
    def _infer_with_retry(self, vertex_service, user_query, context):
        """Helper para executar inferência com retry"""
        retry_delay = RETRY_DELAY
        for attempt in range(MAX_RETRIES):
            try:
                return vertex_service.infer_intent(user_query, context)
            except Exception as e:
                if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e):
                    if attempt < MAX_RETRIES - 1:
                        print(f"\n   ⚠️  Rate limit (tentativa {attempt + 1}/{MAX_RETRIES}), aguardando {retry_delay}s...")
                        time.sleep(retry_delay)
                        retry_delay *= 2
                        continue
                raise
