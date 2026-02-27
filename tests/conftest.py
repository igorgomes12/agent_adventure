"""
Configuração do Pytest
=======================
Fixtures e configurações compartilhadas para os testes.
"""

import pytest
import sys
from pathlib import Path

# Adicionar o diretório raiz ao path
root_dir = Path(__file__).parent.parent
sys.path.insert(0, str(root_dir))


@pytest.fixture
def mock_firestore_data():
    """Fixture com dados de exemplo do Firestore"""
    return {
        "flow_id": "FLUXO_PROPOSTA_VEICULO_CRM",
        "semantic_profile": {
            "description": "Flow de propostas de veículos CRM"
        },
        "table_definition": {
            "table_name": "TbProposta",
            "schema": "dbo",
            "description": "Tabela de propostas",
            "columns": [
                {
                    "name": "NuProposta",
                    "type": "decimal(9,0)",
                    "nullable": False,
                    "description": "Número da proposta"
                },
                {
                    "name": "CdProduto",
                    "type": "int",
                    "nullable": False,
                    "description": "Código do produto"
                },
                {
                    "name": "StatusProposta",
                    "type": "varchar(30)",
                    "nullable": True,
                    "description": "Status da proposta"
                }
            ],
            "constraints": {
                "primary_key": {
                    "columns": ["NuProposta", "CdProduto"]
                },
                "foreign_keys": [
                    {
                        "name": "FK_TbProposta_TbPessoa",
                        "from_columns": ["IdPessoa"],
                        "to_table": "TbPessoa",
                        "to_columns": ["IdPessoa"]
                    }
                ]
            }
        }
    }


@pytest.fixture
def mock_credentials_path(tmp_path):
    """Fixture que cria um arquivo temporário de credenciais"""
    creds_file = tmp_path / "test-credentials.json"
    creds_file.write_text('{"type": "service_account", "project_id": "test"}')
    return str(creds_file)


@pytest.fixture
def mock_vertex_context():
    """Fixture com contexto de exemplo para Vertex AI"""
    return {
        "flow_id": "FLUXO_PROPOSTA_VEICULO_CRM",
        "table": "TbProposta",
        "columns": [
            "NuProposta",
            "CdProduto",
            "StatusProposta",
            "DtCriacao",
            "IdPessoa"
        ],
        "ddl": """
        CREATE TABLE TbProposta (
            NuProposta decimal(9,0) NOT NULL,
            CdProduto int NOT NULL,
            StatusProposta varchar(30) NULL,
            DtCriacao datetime NULL,
            IdPessoa int NULL,
            PRIMARY KEY (NuProposta, CdProduto)
        )
        """
    }


@pytest.fixture
def mock_vertex_response():
    """Fixture com resposta de exemplo do Vertex AI"""
    return {
        "filters": [
            {
                "column": "StatusProposta",
                "operator": "=",
                "value": "Aprovada",
                "nl_term": "aprovadas",
                "confidence": 0.95
            }
        ],
        "select_columns": [],
        "order_by": [{"column": "NuProposta", "direction": "DESC"}],
        "limit": 10,
        "confidence_score": 0.9,
        "reasoning": "Filtro por status aprovada com ordenação por número"
    }
