"""
Testes Reais - Firestore Repository
====================================
Testa a conexão e operações com o Firestore usando dados reais.
Requer .env configurado com credenciais do Firestore.
"""

import pytest
import os
from dotenv import load_dotenv
from src.repositories.firestore_firebase_repository import (
    FirestoreFirebaseRepository,
    HybridFirebaseRepository
)

# Carregar variáveis de ambiente
load_dotenv()


@pytest.fixture(scope="module")
def firestore_repo():
    """Fixture que cria repositório Firestore real"""
    project_id = os.getenv("FIRESTORE_PROJECT_ID") or os.getenv("GCP_PROJECT_ID")
    credentials_path = os.getenv("FIRESTORE_CREDENTIALS") or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    database = os.getenv("FIRESTORE_DATABASE", "(default)")
    
    if not project_id:
        pytest.skip("Projeto Firestore não configurado no .env")
    
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


class TestFirestoreConnection:
    """Testes de conexão com Firestore"""
    
    def test_firestore_initialization(self, firestore_repo):
        """Testa que o repositório foi inicializado corretamente"""
        assert firestore_repo is not None
        assert firestore_repo.db is not None
        assert firestore_repo.fluxos_ref is not None
        print(f"\n   Coleção: fluxos_automotivos")
    
    def test_firestore_client_configured(self, firestore_repo):
        """Testa que o cliente Firestore está configurado"""
        assert firestore_repo.db.project is not None
        print(f"\n   Projeto: {firestore_repo.db.project}")


class TestFirestoreFlowOperations:
    """Testes de operações com flows"""
    
    def test_get_all_flows(self, firestore_repo):
        """Testa buscar todos os flows"""
        flows = firestore_repo.get_all_flows()
        
        assert flows is not None
        assert isinstance(flows, dict)
        
        print(f"\n   Total de flows: {len(flows)}")
        
        if flows:
            print(f"   Flows disponíveis:")
            for flow_id in flows.keys():
                print(f"     - {flow_id}")
        
        assert len(flows) > 0, "Nenhum flow encontrado no Firestore"
    
    def test_get_specific_flow(self, firestore_repo):
        """Testa buscar flow específico"""
        flow_id = "FLUXO_PROPOSTA_VEICULO_CRM"
        flow = firestore_repo.get_flow(flow_id)
        
        if flow:
            print(f"\n   Flow encontrado: {flow_id}")
            print(f"   Descrição: {flow.get('description', 'N/A')}")
            print(f"   Source: {flow.get('source', 'N/A')}")
            
            assert flow["flow_id"] == flow_id
            assert flow["source"] == "firebase"
            assert "original_data" in flow
        else:
            pytest.skip(f"Flow {flow_id} não encontrado")
    
    def test_get_flow_with_metadata(self, firestore_repo):
        """Testa que o flow retorna metadados completos"""
        # Buscar primeiro flow disponível
        all_flows = firestore_repo.get_all_flows()
        
        if not all_flows:
            pytest.skip("Nenhum flow disponível")
        
        flow_id = list(all_flows.keys())[0]
        flow = firestore_repo.get_flow(flow_id)
        
        print(f"\n   Flow: {flow_id}")
        print(f"   Campos disponíveis: {list(flow.keys())}")
        
        assert "flow_id" in flow
        assert "description" in flow
        assert "source" in flow
        assert "original_data" in flow


class TestFirestoreTableOperations:
    """Testes de operações com tabelas"""
    
    def test_get_table_definition(self, firestore_repo):
        """Testa buscar definição de tabela"""
        flow_id = "FLUXO_PROPOSTA_VEICULO_CRM"
        
        # Buscar documento do flow
        doc = firestore_repo.fluxos_ref.document(flow_id).get()
        
        if not doc.exists:
            pytest.skip(f"Flow {flow_id} não encontrado")
        
        firebase_data = doc.to_dict()
        
        if "table_definition" in firebase_data:
            table_def = firebase_data["table_definition"]
            
            print(f"\n   Flow: {flow_id}")
            print(f"   Tabela: {table_def.get('table_name', 'N/A')}")
            print(f"   Schema: {table_def.get('schema', 'N/A')}")
            print(f"   Colunas: {len(table_def.get('columns', []))}")
            
            assert "table_name" in table_def
            assert "columns" in table_def
            assert len(table_def["columns"]) > 0
        else:
            pytest.skip("table_definition não encontrado no flow")
    
    def test_get_table_columns(self, firestore_repo):
        """Testa buscar colunas da tabela"""
        flow_id = "FLUXO_PROPOSTA_VEICULO_CRM"
        
        doc = firestore_repo.fluxos_ref.document(flow_id).get()
        
        if not doc.exists:
            pytest.skip(f"Flow {flow_id} não encontrado")
        
        firebase_data = doc.to_dict()
        table_def = firebase_data.get("table_definition", {})
        columns = table_def.get("columns", [])
        
        print(f"\n   Total de colunas: {len(columns)}")
        
        if columns:
            print(f"   Primeiras 5 colunas:")
            for col in columns[:5]:
                print(f"     - {col.get('name')}: {col.get('type')} (nullable: {col.get('nullable')})")
        
        assert len(columns) > 0
    
    def test_get_table_constraints(self, firestore_repo):
        """Testa buscar constraints da tabela"""
        flow_id = "FLUXO_PROPOSTA_VEICULO_CRM"
        
        doc = firestore_repo.fluxos_ref.document(flow_id).get()
        
        if not doc.exists:
            pytest.skip(f"Flow {flow_id} não encontrado")
        
        firebase_data = doc.to_dict()
        table_def = firebase_data.get("table_definition", {})
        constraints = table_def.get("constraints", {})
        
        print(f"\n   Constraints:")
        
        if "primary_key" in constraints:
            pk = constraints["primary_key"]
            pk_cols = pk.get("columns", []) if isinstance(pk, dict) else pk
            print(f"     Primary Key: {pk_cols}")
        
        if "foreign_keys" in constraints:
            fks = constraints["foreign_keys"]
            print(f"     Foreign Keys: {len(fks)}")
            for fk in fks[:3]:  # Mostrar primeiras 3
                print(f"       - {fk.get('name')}: {fk.get('from_columns')} -> {fk.get('to_table')}")
    
    def test_get_table_using_repository_method(self, firestore_repo):
        """Testa buscar tabela usando método do repositório"""
        flow_id = "FLUXO_PROPOSTA_VEICULO_CRM"
        table_name = "TbProposta"
        
        table = firestore_repo.get_table(flow_id, table_name)
        
        if table:
            print(f"\n   Tabela: {table_name}")
            print(f"   Flow: {flow_id}")
            print(f"   Colunas: {len(table.get('columns_dictionary', []))}")
            
            assert table["flow_id"] == flow_id
            assert table["table_profile"]["table_name"] == table_name
            assert "columns_dictionary" in table
        else:
            pytest.skip(f"Tabela {table_name} não encontrada")


class TestFirestoreDDLOperations:
    """Testes de operações com DDL"""
    
    def test_get_ddl(self, firestore_repo):
        """Testa buscar DDL de uma tabela"""
        flow_id = "FLUXO_PROPOSTA_VEICULO_CRM"
        table_name = "TbProposta"
        
        ddl = firestore_repo.get_ddl(flow_id, table_name)
        
        if ddl:
            print(f"\n   DDL da tabela: {table_name}")
            print(f"   Schema: {ddl.get('schema', 'N/A')}")
            print(f"   Colunas: {len(ddl.get('columns', []))}")
            print(f"   Primary Key: {ddl.get('constraints', {}).get('primary_key', [])}")
            print(f"   Foreign Keys: {len(ddl.get('constraints', {}).get('foreign_keys', []))}")
            
            assert ddl["table_name"] == table_name
            assert "columns" in ddl
            assert "constraints" in ddl
        else:
            pytest.skip(f"DDL não encontrado para {table_name}")
    
    def test_ddl_columns_format(self, firestore_repo):
        """Testa formato das colunas no DDL"""
        flow_id = "FLUXO_PROPOSTA_VEICULO_CRM"
        table_name = "TbProposta"
        
        ddl = firestore_repo.get_ddl(flow_id, table_name)
        
        if not ddl:
            pytest.skip("DDL não encontrado")
        
        columns = ddl.get("columns", [])
        
        if columns:
            first_col = columns[0]
            print(f"\n   Exemplo de coluna:")
            print(f"     Nome: {first_col.get('name')}")
            print(f"     Tipo: {first_col.get('type')}")
            print(f"     Nullable: {first_col.get('nullable')}")
            
            assert "name" in first_col
            assert "type" in first_col
            assert "nullable" in first_col


class TestFirestoreDataIntegrity:
    """Testes de integridade dos dados"""
    
    def test_flow_has_required_fields(self, firestore_repo):
        """Testa que flows têm campos obrigatórios"""
        all_flows = firestore_repo.get_all_flows()
        
        if not all_flows:
            pytest.skip("Nenhum flow disponível")
        
        flow_id = list(all_flows.keys())[0]
        flow = all_flows[flow_id]
        
        print(f"\n   Verificando flow: {flow_id}")
        
        # Campos obrigatórios
        assert "flow_id" in flow, "Campo flow_id ausente"
        assert "description" in flow, "Campo description ausente"
        assert "source" in flow, "Campo source ausente"
        
        print(f"   ✓ Todos os campos obrigatórios presentes")
    
    def test_table_definition_structure(self, firestore_repo):
        """Testa estrutura da definição de tabela"""
        flow_id = "FLUXO_PROPOSTA_VEICULO_CRM"
        
        doc = firestore_repo.fluxos_ref.document(flow_id).get()
        
        if not doc.exists:
            pytest.skip("Flow não encontrado")
        
        firebase_data = doc.to_dict()
        table_def = firebase_data.get("table_definition")
        
        if not table_def:
            pytest.skip("table_definition não encontrado")
        
        print(f"\n   Verificando estrutura da tabela")
        
        # Campos obrigatórios
        assert "table_name" in table_def, "Campo table_name ausente"
        assert "columns" in table_def, "Campo columns ausente"
        
        # Verificar estrutura das colunas
        columns = table_def["columns"]
        if columns:
            first_col = columns[0]
            assert "name" in first_col, "Campo name ausente na coluna"
            assert "type" in first_col, "Campo type ausente na coluna"
        
        print(f"   ✓ Estrutura válida")
    
    def test_data_consistency(self, firestore_repo):
        """Testa consistência dos dados entre métodos"""
        flow_id = "FLUXO_PROPOSTA_VEICULO_CRM"
        
        # Buscar flow de duas formas diferentes
        flow1 = firestore_repo.get_flow(flow_id)
        
        all_flows = firestore_repo.get_all_flows()
        flow2 = all_flows.get(flow_id)
        
        if flow1 and flow2:
            print(f"\n   Verificando consistência para: {flow_id}")
            
            assert flow1["flow_id"] == flow2["flow_id"]
            assert flow1["description"] == flow2["description"]
            
            print(f"   ✓ Dados consistentes")


class TestFirestoreErrorHandling:
    """Testes de tratamento de erros"""
    
    def test_get_nonexistent_flow(self, firestore_repo):
        """Testa buscar flow que não existe"""
        flow_id = "FLOW_INEXISTENTE_12345"
        flow = firestore_repo.get_flow(flow_id)
        
        print(f"\n   Buscando flow inexistente: {flow_id}")
        assert flow is None
        print(f"   ✓ Retornou None corretamente")
    
    def test_get_nonexistent_table(self, firestore_repo):
        """Testa buscar tabela que não existe"""
        flow_id = "FLUXO_PROPOSTA_VEICULO_CRM"
        table_name = "TabelaInexistente"
        
        table = firestore_repo.get_table(flow_id, table_name)
        
        print(f"\n   Buscando tabela inexistente: {table_name}")
        assert table is None
        print(f"   ✓ Retornou None corretamente")
