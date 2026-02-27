"""
Testes Reais - Intent Agent
============================
Testes que mostram o comportamento real de cada passo do agente de intenções.
Usa dados reais do Firestore e Vertex AI.
"""

import pytest
import json
import os
import time
from dotenv import load_dotenv
from src.agent.intent_agent import IntentAgent
from src.strategies.local_strategy import LocalStrategy
from src.strategies.ai_strategy import AIStrategy
from src.services.ai_inference_vertex import AIInferenceServiceVertex
from src.repositories.firestore_firebase_repository import FirestoreFirebaseRepository
from src.models.intent import ProcessStatus, ValidationLevel

# Carregar variáveis de ambiente
load_dotenv()

# Delay entre testes
DELAY_BETWEEN_TESTS = 3
MAX_RETRIES = 3
RETRY_DELAY = 5


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
def vertex_service():
    """Fixture que cria serviço Vertex AI real"""
    project_id = os.getenv("GCP_PROJECT_ID")
    location = os.getenv("GCP_LOCATION", "us-central1")
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    
    if not project_id:
        pytest.skip("GCP_PROJECT_ID não configurado")
    
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
def intent_agent(firestore_repo, vertex_service):
    """Fixture que cria o agente de intenções"""
    local_strategy = LocalStrategy(firestore_repo)
    ai_strategy = AIStrategy(vertex_service)
    
    agent = IntentAgent(
        repository=firestore_repo,
        local_strategy=local_strategy,
        ai_strategy=ai_strategy,
        gemini_threshold=0.5
    )
    
    print(f"\n✅ Intent Agent criado")
    return agent


class TestIntentAgentInitialization:
    """Testes de inicialização do agente"""
    
    def test_agent_initialization(self, intent_agent):
        """Testa que o agente foi inicializado corretamente"""
        assert intent_agent is not None
        assert intent_agent.repo is not None
        assert intent_agent.local_strategy is not None
        assert intent_agent.ai_strategy is not None
        assert intent_agent.validator is not None
        assert intent_agent.gemini_threshold == 0.5
        
        print(f"\n   Threshold Gemini: {intent_agent.gemini_threshold}")


class TestIntentAgentValidation:
    """Testes de validação (Camada 1 - Crítica)"""
    
    def test_step1_validate_existing_flow_and_table(self, intent_agent):
        """STEP 1: Valida flow e tabela que existem"""
        flow_id = "FLUXO_PROPOSTA_VEICULO_CRM"
        table_name = "TbProposta"
        
        print(f"\n   === STEP 1: VALIDAÇÃO CRÍTICA ===")
        print(f"   Flow: {flow_id}")
        print(f"   Tabela: {table_name}")
        
        exists, warnings = intent_agent.validator.validate_flow_and_table(flow_id, table_name)
        
        print(f"   Resultado: {'✅ VÁLIDO' if exists else '❌ INVÁLIDO'}")
        print(f"   Warnings: {len(warnings)}")
        
        assert exists is True
        assert len(warnings) == 0
    
    def test_step1_validate_nonexistent_flow(self, intent_agent):
        """STEP 1: Valida flow que não existe (deve falhar)"""
        flow_id = "FLOW_INEXISTENTE_12345"
        table_name = "TbProposta"
        
        print(f"\n   === STEP 1: VALIDAÇÃO CRÍTICA (Flow Inexistente) ===")
        print(f"   Flow: {flow_id}")
        
        exists, warnings = intent_agent.validator.validate_flow_and_table(flow_id, table_name)
        
        print(f"   Resultado: {'✅ VÁLIDO' if exists else '❌ INVÁLIDO'}")
        print(f"   Warnings: {len(warnings)}")
        
        if warnings:
            for w in warnings:
                print(f"     [{w.level.value}] {w.message}")
        
        assert exists is False
        assert len(warnings) > 0
        assert warnings[0].level == ValidationLevel.CRITICAL
    
    def test_step1_validate_nonexistent_table(self, intent_agent):
        """STEP 1: Valida tabela que não existe (deve falhar)"""
        flow_id = "FLUXO_PROPOSTA_VEICULO_CRM"
        table_name = "TabelaInexistente"
        
        print(f"\n   === STEP 1: VALIDAÇÃO CRÍTICA (Tabela Inexistente) ===")
        print(f"   Tabela: {table_name}")
        
        exists, warnings = intent_agent.validator.validate_flow_and_table(flow_id, table_name)
        
        print(f"   Resultado: {'✅ VÁLIDO' if exists else '❌ INVÁLIDO'}")
        print(f"   Warnings: {len(warnings)}")
        
        if warnings:
            for w in warnings:
                print(f"     [{w.level.value}] {w.message}")
        
        assert exists is False
        assert len(warnings) > 0


class TestIntentAgentDDLLoading:
    """Testes de carregamento de DDL (Step 2)"""
    
    def test_step2_load_ddl(self, intent_agent):
        """STEP 2: Carrega DDL da tabela"""
        flow_id = "FLUXO_PROPOSTA_VEICULO_CRM"
        table_name = "TbProposta"
        
        print(f"\n   === STEP 2: CARREGAR DDL ===")
        
        ddl_data = intent_agent.repo.get_ddl(flow_id, table_name)
        
        assert ddl_data is not None
        assert "table_name" in ddl_data
        assert "columns" in ddl_data
        assert "constraints" in ddl_data
        
        print(f"   Tabela: {ddl_data.get('schema', 'dbo')}.{ddl_data['table_name']}")
        print(f"   Colunas: {len(ddl_data['columns'])}")
        print(f"   Primary Key: {ddl_data.get('constraints', {}).get('primary_key', [])}")
        print(f"   Foreign Keys: {len(ddl_data.get('constraints', {}).get('foreign_keys', []))}")


class TestIntentAgentStrategySelection:
    """Testes de seleção de estratégia (Step 4)"""
    
    def test_step4_select_ai_strategy_low_score(self, intent_agent):
        """STEP 4: Seleciona estratégia AI quando score é baixo"""
        flow_score = 3.0  # Baixo (< 5.0)
        user_prompt = "mostre as propostas aprovadas"
        flow_id = "FLUXO_PROPOSTA_VEICULO_CRM"
        table_name = "TbProposta"
        
        print(f"\n   === STEP 4: SELEÇÃO DE ESTRATÉGIA ===")
        print(f"   Score: {flow_score}")
        print(f"   Threshold: {intent_agent.gemini_threshold * 10}")
        
        strategy = intent_agent._select_strategy(flow_score, user_prompt, flow_id, table_name)
        
        print(f"   Estratégia selecionada: {type(strategy).__name__}")
        
        assert isinstance(strategy, AIStrategy)
    
    def test_step4_select_local_strategy_high_score(self, intent_agent):
        """STEP 4: Seleciona estratégia local quando score é alto"""
        flow_score = 9.0  # Alto (>= 5.0)
        user_prompt = "mostre as propostas aprovadas"
        flow_id = "FLUXO_PROPOSTA_VEICULO_CRM"
        table_name = "TbProposta"
        
        print(f"\n   === STEP 4: SELEÇÃO DE ESTRATÉGIA ===")
        print(f"   Score: {flow_score}")
        print(f"   Threshold: {intent_agent.gemini_threshold * 10}")
        
        strategy = intent_agent._select_strategy(flow_score, user_prompt, flow_id, table_name)
        
        print(f"   Estratégia selecionada: {type(strategy).__name__}")
        
        assert isinstance(strategy, LocalStrategy)


class TestIntentAgentProcessSimple:
    """Testes de processamento simples (workflow completo)"""
    
    def test_process_simple_query_success(self, intent_agent):
        """Testa processamento completo de query simples"""
        time.sleep(DELAY_BETWEEN_TESTS)
        
        user_prompt = "mostre as propostas aprovadas"
        flow_id = "FLUXO_PROPOSTA_VEICULO_CRM"
        table_name = "TbProposta"
        flow_score = 9.0
        
        print(f"\n   === WORKFLOW COMPLETO: QUERY SIMPLES ===")
        print(f"   Prompt: \"{user_prompt}\"")
        print(f"   Flow: {flow_id}")
        print(f"   Tabela: {table_name}")
        print(f"   Score: {flow_score}")
        
        # Processar com retry
        intent = self._process_with_retry(intent_agent, user_prompt, flow_id, table_name, flow_score)
        
        # Verificações
        print(f"\n   === RESULTADO ===")
        print(f"   Status: {intent.status.value}")
        print(f"   Confidence: {intent.confidence_score:.2f}")
        print(f"   Filtros: {len(intent.filters)}")
        print(f"   Warnings: {len(intent.warnings)}")
        
        if intent.filters:
            for f in intent.filters:
                print(f"     - {f.column} {f.operator} {f.value} (confidence: {f.confidence:.2f})")
        
        if intent.warnings:
            for w in intent.warnings:
                print(f"     [{w.level.value}] {w.message}")
        
        assert intent.status in [ProcessStatus.SUCCESS, ProcessStatus.PARTIAL_SUCCESS]
        assert intent.flow_id == flow_id
        assert intent.table_name == table_name
        assert intent.confidence_score > 0
    
    def _process_with_retry(self, agent, user_prompt, flow_id, table_name, flow_score):
        """Helper para processar com retry"""
        retry_delay = RETRY_DELAY
        for attempt in range(MAX_RETRIES):
            try:
                return agent.process(user_prompt, flow_id, table_name, flow_score)
            except Exception as e:
                if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e):
                    if attempt < MAX_RETRIES - 1:
                        print(f"\n   ⚠️  Rate limit, aguardando {retry_delay}s...")
                        time.sleep(retry_delay)
                        retry_delay *= 2
                        continue
                raise
    
    def test_process_with_date_filter(self, intent_agent):
        """Testa processamento com filtro de data"""
        time.sleep(DELAY_BETWEEN_TESTS)
        
        user_prompt = "propostas dos últimos 30 dias"
        flow_id = "FLUXO_PROPOSTA_VEICULO_CRM"
        table_name = "TbProposta"
        flow_score = 9.0
        
        print(f"\n   === WORKFLOW: FILTRO DE DATA ===")
        print(f"   Prompt: \"{user_prompt}\"")
        
        intent = self._process_with_retry(intent_agent, user_prompt, flow_id, table_name, flow_score)
        
        print(f"\n   Status: {intent.status.value}")
        print(f"   Filtros: {len(intent.filters)}")
        
        if intent.filters:
            for f in intent.filters:
                print(f"     - {f.column} {f.operator} {f.value}")
        
        assert intent.status in [ProcessStatus.SUCCESS, ProcessStatus.PARTIAL_SUCCESS]
        assert len(intent.filters) > 0


class TestIntentAgentProcessError:
    """Testes de processamento com erro"""
    
    def test_process_nonexistent_flow_returns_error(self, intent_agent):
        """Testa que flow inexistente retorna erro"""
        user_prompt = "mostre as propostas"
        flow_id = "FLOW_INEXISTENTE_12345"
        table_name = "TbProposta"
        
        print(f"\n   === WORKFLOW: FLOW INEXISTENTE ===")
        print(f"   Flow: {flow_id}")
        
        intent = intent_agent.process(user_prompt, flow_id, table_name)
        
        print(f"\n   Status: {intent.status.value}")
        print(f"   Warnings: {len(intent.warnings)}")
        
        if intent.warnings:
            for w in intent.warnings:
                print(f"     [{w.level.value}] {w.message}")
        
        assert intent.status == ProcessStatus.ERROR
        assert len(intent.warnings) > 0
        assert intent.warnings[0].level == ValidationLevel.CRITICAL


class TestIntentAgentColumnValidation:
    """Testes de validação de colunas (Camada 2)"""
    
    def test_step6_validate_existing_columns(self, intent_agent):
        """STEP 6: Valida colunas que existem"""
        flow_id = "FLUXO_PROPOSTA_VEICULO_CRM"
        table_name = "TbProposta"
        
        # Carregar DDL
        ddl_data = intent_agent.repo.get_ddl(flow_id, table_name)
        columns = ddl_data.get('columns', [])
        
        # Criar filtros de teste
        from src.models.intent import FilterCondition
        filters = [
            FilterCondition(
                column="StatusProposta",
                operator="=",
                value="Aprovada",
                nl_term="aprovadas",
                resolved_via="test",
                confidence=0.9
            )
        ]
        
        print(f"\n   === STEP 6: VALIDAÇÃO DE COLUNAS ===")
        print(f"   Filtros para validar: {len(filters)}")
        
        validated_filters, warnings = intent_agent.validator.validate_columns(filters, columns)
        
        print(f"   Filtros validados: {len(validated_filters)}")
        print(f"   Warnings: {len(warnings)}")
        
        assert len(validated_filters) == len(filters)
        assert len(warnings) == 0
    
    def test_step6_validate_nonexistent_column(self, intent_agent):
        """STEP 6: Valida coluna que não existe"""
        flow_id = "FLUXO_PROPOSTA_VEICULO_CRM"
        table_name = "TbProposta"
        
        ddl_data = intent_agent.repo.get_ddl(flow_id, table_name)
        columns = ddl_data.get('columns', [])
        
        from src.models.intent import FilterCondition
        filters = [
            FilterCondition(
                column="ColunaInexistente",
                operator="=",
                value="teste",
                nl_term="teste",
                resolved_via="test",
                confidence=0.9
            )
        ]
        
        print(f"\n   === STEP 6: VALIDAÇÃO (Coluna Inexistente) ===")
        
        validated_filters, warnings = intent_agent.validator.validate_columns(filters, columns)
        
        print(f"   Warnings: {len(warnings)}")
        
        if warnings:
            for w in warnings:
                print(f"     [{w.level.value}] {w.message}")
                if w.suggestions:
                    print(f"       Sugestões: {', '.join(w.suggestions[:3])}")
        
        assert len(warnings) > 0
        assert warnings[0].level == ValidationLevel.WARNING


class TestIntentAgentOutputFormat:
    """Testes de formato de saída"""
    
    def test_intent_to_dict(self, intent_agent):
        """Testa conversão do intent para dicionário"""
        time.sleep(DELAY_BETWEEN_TESTS)
        
        user_prompt = "mostre as propostas aprovadas"
        flow_id = "FLUXO_PROPOSTA_VEICULO_CRM"
        table_name = "TbProposta"
        
        intent = self._process_with_retry(intent_agent, user_prompt, flow_id, table_name, 9.0)
        
        intent_dict = intent.to_dict()
        
        print(f"\n   === FORMATO DE SAÍDA ===")
        print(f"   Campos: {list(intent_dict.keys())}")
        
        # Verificar campos obrigatórios
        assert "flow_id" in intent_dict
        assert "table_name" in intent_dict
        assert "filters" in intent_dict
        assert "confidence_score" in intent_dict
        assert "status" in intent_dict
        assert "warnings" in intent_dict
        assert "ddl_reference" in intent_dict
        assert "original_prompt" in intent_dict
    
    def test_intent_to_json(self, intent_agent):
        """Testa conversão do intent para JSON"""
        time.sleep(DELAY_BETWEEN_TESTS)
        
        user_prompt = "mostre as propostas"
        flow_id = "FLUXO_PROPOSTA_VEICULO_CRM"
        table_name = "TbProposta"
        
        intent = self._process_with_retry(intent_agent, user_prompt, flow_id, table_name, 9.0)
        
        intent_json = intent.to_json()
        
        print(f"\n   === JSON OUTPUT ===")
        print(f"   Tamanho: {len(intent_json)} caracteres")
        
        # Verificar que é JSON válido
        parsed = json.loads(intent_json)
        assert parsed["flow_id"] == flow_id
        assert parsed["table_name"] == table_name
    
    def _process_with_retry(self, agent, user_prompt, flow_id, table_name, flow_score):
        """Helper para processar com retry"""
        retry_delay = RETRY_DELAY
        for attempt in range(MAX_RETRIES):
            try:
                return agent.process(user_prompt, flow_id, table_name, flow_score)
            except Exception as e:
                if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e):
                    if attempt < MAX_RETRIES - 1:
                        print(f"\n   ⚠️  Rate limit, aguardando {retry_delay}s...")
                        time.sleep(retry_delay)
                        retry_delay *= 2
                        continue
                raise


class TestIntentAgentCDCVeiculosLeves:
    """Teste específico: propostas CDC com veículos leves em atraso > 60 dias"""

    def _process_with_retry(self, agent, user_prompt, flow_id, table_name, flow_score):
        """Helper para processar com retry em caso de rate limiting"""
        retry_delay = RETRY_DELAY
        for attempt in range(MAX_RETRIES):
            try:
                return agent.process(user_prompt, flow_id, table_name, flow_score)
            except Exception as e:
                if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e):
                    if attempt < MAX_RETRIES - 1:
                        print(f"\n   ⚠️  Rate limit (tentativa {attempt + 1}/{MAX_RETRIES}), aguardando {retry_delay}s...")
                        time.sleep(retry_delay)
                        retry_delay *= 2
                        continue
                raise

    def test_cdc_veiculos_leves_atraso_60_dias(self, intent_agent):
        """
        Testa o agente com a query:
        'propostas CDC com veículos leves em atraso com mais de 60 dias'

        Verifica cada passo do processamento:
          STEP 1 - Validação crítica (flow + tabela)
          STEP 2 - Carregamento do DDL
          STEP 3 - Montagem dos matches
          STEP 4 - Seleção de estratégia
          STEP 5 - Construção do intent
          STEP 6 - Validação de colunas
          STEP 7 - Status e confidence final
        """
        time.sleep(DELAY_BETWEEN_TESTS)

        user_prompt = "propostas CDC com veículos leves em atraso com mais de 60 dias"
        flow_id    = "FLUXO_PROPOSTA_VEICULO_CRM"
        table_name = "TbProposta"
        flow_score = 9.0

        print(f"\n{'='*60}")
        print(f"  TESTE: CDC / Veículos Leves / Atraso > 60 dias")
        print(f"{'='*60}")
        print(f"  Prompt : \"{user_prompt}\"")
        print(f"  Flow   : {flow_id}")
        print(f"  Tabela : {table_name}")
        print(f"  Score  : {flow_score}")

        # ── STEP 1: validação crítica ──────────────────────────────
        print(f"\n  [STEP 1] Validação crítica (flow + tabela)")
        exists, val_warnings = intent_agent.validator.validate_flow_and_table(flow_id, table_name)
        print(f"           Resultado : {'✅ OK' if exists else '❌ FALHOU'}")
        print(f"           Warnings  : {len(val_warnings)}")
        assert exists, "Flow ou tabela não encontrados no Firestore"

        # ── STEP 2: DDL ────────────────────────────────────────────
        print(f"\n  [STEP 2] Carregamento do DDL")
        ddl_data = intent_agent.repo.get_ddl(flow_id, table_name)
        assert ddl_data is not None, "DDL não encontrado"
        print(f"           Schema  : {ddl_data.get('schema', 'dbo')}.{table_name}")
        print(f"           Colunas : {len(ddl_data.get('columns', []))}")
        col_names = [c['name'] for c in ddl_data.get('columns', [])]
        print(f"           Nomes   : {col_names}")

        # ── STEP 3: matches ────────────────────────────────────────
        print(f"\n  [STEP 3] Montagem dos matches")
        flow_match  = {'flow_id': flow_id, '_match_score': flow_score}
        table_match = {'table_profile': {'table_name': table_name}}
        print(f"           flow_match  : {flow_match}")
        print(f"           table_match : {table_match}")

        # ── STEP 4: estratégia ─────────────────────────────────────
        print(f"\n  [STEP 4] Seleção de estratégia")
        strategy = intent_agent._select_strategy(flow_score, user_prompt, flow_id, table_name)
        print(f"           Estratégia : {type(strategy).__name__}")

        # ── STEP 5-7: processamento completo ───────────────────────
        print(f"\n  [STEP 5-7] Processamento completo (build_intent + validações)")
        intent = self._process_with_retry(intent_agent, user_prompt, flow_id, table_name, flow_score)

        # ── Resultado ──────────────────────────────────────────────
        print(f"\n  {'─'*55}")
        print(f"  RESULTADO FINAL")
        print(f"  {'─'*55}")
        print(f"  Status     : {intent.status.value}")
        print(f"  Confidence : {intent.confidence_score:.2f}")
        print(f"  Filtros    : {len(intent.filters)}")

        for i, f in enumerate(intent.filters, 1):
            print(f"    [{i}] {f.column} {f.operator} {f.value}")
            print(f"         nl_term      : {f.nl_term}")
            print(f"         resolved_via : {f.resolved_via}")
            print(f"         confidence   : {f.confidence:.2f}")
            print(f"         validated    : {f.validated}")

        print(f"\n  Order By   : {intent.order_by}")
        print(f"  Limit      : {intent.limit}")
        print(f"  Warnings   : {len(intent.warnings)}")

        for w in intent.warnings:
            print(f"    [{w.level.value}] {w.message}")
            if w.suggestions:
                print(f"      Sugestões: {', '.join(w.suggestions[:3])}")

        print(f"\n  JSON completo:")
        print(intent.to_json())

        # ── Assertions ─────────────────────────────────────────────
        assert intent.status in [ProcessStatus.SUCCESS, ProcessStatus.PARTIAL_SUCCESS]
        assert intent.flow_id    == flow_id
        assert intent.table_name == table_name
        assert intent.confidence_score > 0
        assert len(intent.filters) > 0, "Esperado ao menos 1 filtro para a query"

        # Deve ter detectado algum filtro relacionado a atraso/data
        filter_cols = [f.column for f in intent.filters]
        filter_vals = [str(f.value).lower() for f in intent.filters]
        print(f"\n  Colunas filtradas : {filter_cols}")
        print(f"  Valores filtrados : {filter_vals}")

        # Verifica que algum filtro temporal foi gerado (DATEADD ou valor numérico 60)
        has_temporal = any(
            "DATEADD" in str(f.value) or "60" in str(f.value)
            for f in intent.filters
        )
        assert has_temporal, (
            "Esperado filtro temporal (DATEADD ou 60 dias) para 'atraso com mais de 60 dias'"
        )

        # ── Saída no contrato do agente de query ───────────────────
        print(f"\n  {'─'*55}")
        print(f"  OUTPUT (contrato agente de query)")
        print(f"  {'─'*55}")
        output_str = intent.to_output(repository=intent_agent.repo)
        print(output_str)

        output = json.loads(output_str)

        # Verificar estrutura do contrato
        assert "parameters"    in output
        assert "ddl"           in output
        assert "filter_fields" in output["parameters"]
        assert "return_fields" in output["parameters"]
        assert "tables"        in output["ddl"]

        # filter_fields devem usar schema.tabela.coluna
        for ff in output["parameters"]["filter_fields"]:
            key = list(ff.keys())[0]
            parts = key.split(".")
            assert len(parts) == 3, f"filter_field deve ser schema.tabela.coluna, got: {key}"

        # return_fields devem usar schema.tabela.coluna
        for rf in output["parameters"]["return_fields"]:
            parts = rf.split(".")
            assert len(parts) == 3, f"return_field deve ser schema.tabela.coluna, got: {rf}"

        # DDL deve ter ao menos a tabela principal
        table_names = [t["name"] for t in output["ddl"]["tables"]]
        assert table_name in table_names, f"{table_name} não encontrada no DDL"
        print(f"\n  Tabelas no DDL: {table_names}")
