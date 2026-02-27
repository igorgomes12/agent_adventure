"""
Testes unitários — convert_intent_to_query_format, _build_table_entry, _fetch_related_tables
"""
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime
from src.models.query_output import (
    convert_intent_to_query_format, _build_table_entry, _fetch_related_tables
)
from src.models.intent import (
    IntentObject, FilterCondition, DDLReference, ProcessStatus
)


# ── fixtures ──────────────────────────────────────────────────────────────────

COLUMNS = [
    {"name": "NuProposta",     "type": "decimal(9,0)", "nullable": False},
    {"name": "StatusProposta", "type": "varchar(30)",  "nullable": True},
    {"name": "DtCriacao",      "type": "datetime",     "nullable": True},
]

CONSTRAINTS = {
    "primary_key": ["NuProposta"],
    "foreign_keys": [
        {
            "name": "FK_TbProposta_TbPessoa",
            "column": "IdPessoa",
            "references": {"table": "TbPessoa", "column": "IdPessoa"}
        }
    ]
}


def _ddl_ref(schema="dbo", columns=None, constraints=None):
    return DDLReference(
        flow_id="FLOW_A",
        table_name="TbProposta",
        schema=schema,
        ddl_hash="abc123",
        columns_available=columns or COLUMNS,
        constraints=constraints or CONSTRAINTS,
        validated_at=datetime.now().isoformat()
    )


def _intent(filters=None, select_columns=None, blocked=None):
    return IntentObject(
        flow_id="FLOW_A",
        table_name="TbProposta",
        intent_type="massa_para_teste",
        filters=filters or [],
        select_columns=select_columns or [],
        joins=[],
        order_by=[],
        limit=3,
        confidence_score=0.85,
        metadata={
            "schema": "dbo",
            "database_type": "SYBASE",
            "blocked_columns": blocked or [],
            "database_name": "mydb"
        },
        ddl_reference=_ddl_ref(),
        sources_consulted={},
        original_prompt="test",
        created_at=datetime.now().isoformat(),
        status=ProcessStatus.SUCCESS
    )


def _filter(col, op="=", val="X"):
    return FilterCondition(
        column=col, operator=op, value=val,
        nl_term=col, resolved_via="test"
    )


# ── _build_table_entry ────────────────────────────────────────────────────────

class TestBuildTableEntry:

    def test_basic_structure(self):
        entry = _build_table_entry("dbo", "TbProposta", COLUMNS, {})
        assert entry["schema"] == "dbo"
        assert entry["name"] == "TbProposta"
        assert len(entry["columns"]) == 3

    def test_primary_key_included(self):
        entry = _build_table_entry("dbo", "TbProposta", COLUMNS, CONSTRAINTS)
        assert "primaryKey" in entry
        assert "NuProposta" in entry["primaryKey"]

    def test_foreign_keys_included(self):
        entry = _build_table_entry("dbo", "TbProposta", COLUMNS, CONSTRAINTS)
        assert "foreignKeys" in entry
        fk = entry["foreignKeys"][0]
        assert fk["column"] == "IdPessoa"
        assert fk["references"]["table"] == "TbPessoa"

    def test_no_pk_when_empty(self):
        entry = _build_table_entry("dbo", "TbProposta", COLUMNS, {})
        assert "primaryKey" not in entry

    def test_no_fk_when_empty(self):
        entry = _build_table_entry("dbo", "TbProposta", COLUMNS, {"primary_key": ["NuProposta"]})
        assert "foreignKeys" not in entry

    def test_column_fields(self):
        entry = _build_table_entry("dbo", "TbProposta", COLUMNS, {})
        col = entry["columns"][0]
        assert "name" in col
        assert "type" in col
        assert "nullable" in col


# ── convert_intent_to_query_format ────────────────────────────────────────────

class TestConvertIntentToQueryFormat:

    def test_output_keys(self):
        result = convert_intent_to_query_format(_intent())
        assert "parameters" in result
        assert "ddl" in result
        assert "filter_fields" in result["parameters"]
        assert "tables" in result["parameters"]
        assert "return_fields" in result["parameters"]

    def test_filter_fields_format(self):
        f = _filter("StatusProposta", "=", "Aprovada")
        result = convert_intent_to_query_format(_intent(filters=[f]))
        ff = result["parameters"]["filter_fields"]
        assert len(ff) == 1
        key = list(ff[0].keys())[0]
        assert key == "mydb.dbo.TbProposta.StatusProposta"
        assert ff[0][key] == "= 'Aprovada'"  # Valores string devem ter aspas

    def test_return_fields_from_ddl_when_no_select(self):
        result = convert_intent_to_query_format(_intent())
        rf = result["parameters"]["return_fields"]
        assert "mydb.dbo.TbProposta.NuProposta" in rf
        assert "mydb.dbo.TbProposta.StatusProposta" in rf

    def test_return_fields_from_select_columns(self):
        result = convert_intent_to_query_format(_intent(select_columns=["NuProposta"]))
        rf = result["parameters"]["return_fields"]
        assert rf == ["mydb.dbo.TbProposta.NuProposta"]

    def test_blocked_columns_excluded(self):
        result = convert_intent_to_query_format(_intent(blocked=["DtCriacao"]))
        rf = result["parameters"]["return_fields"]
        assert "mydb.dbo.TbProposta.DtCriacao" not in rf

    def test_tables_list_included(self):
        result = convert_intent_to_query_format(_intent())
        tables = result["parameters"]["tables"]
        assert isinstance(tables, list)
        assert "mydb.dbo.TbProposta" in tables

    def test_ddl_is_array(self):
        result = convert_intent_to_query_format(_intent())
        ddl = result["ddl"]
        assert isinstance(ddl, list)
        assert len(ddl) == 1

    def test_ddl_database_and_tipo(self):
        result = convert_intent_to_query_format(_intent())
        ddl = result["ddl"][0]
        assert ddl["database"] == "mydb"
        assert ddl["tipo"] == "SYBASE"

    def test_ddl_tables_has_main_table(self):
        result = convert_intent_to_query_format(_intent())
        tables = result["ddl"][0]["tables"]
        assert len(tables) >= 1
        assert tables[0]["name"] == "TbProposta"

    def test_no_repository_no_related_tables(self):
        result = convert_intent_to_query_format(_intent(), repository=None)
        assert len(result["ddl"][0]["tables"]) == 1
        assert len(result["parameters"]["tables"]) == 1

    def test_with_repository_calls_fetch_related(self):
        # repository fornecido mas sem hints → ainda retorna só a tabela principal
        repo = MagicMock()
        doc = MagicMock()
        doc.exists = True
        doc.to_dict.return_value = {"ai_and_rag_support": {"relationships_hints": {"outgoing": []}}}
        repo.fluxos_ref.document.return_value.get.return_value = doc
        result = convert_intent_to_query_format(_intent(), repository=repo)
        assert len(result["ddl"][0]["tables"]) == 1
        assert len(result["parameters"]["tables"]) == 1


# ── _fetch_related_tables ─────────────────────────────────────────────────────

class TestFetchRelatedTables:

    def _make_repo_with_hints(self, hints, related_ddl=None):
        repo = MagicMock()
        
        # Mock get_tables_by_flow para retornar a tabela principal com hints
        main_table = {
            "table_profile": {
                "table_name": "TbProposta"
            },
            "relationships": {
                "outgoing": hints
            }
        }
        repo.get_tables_by_flow.return_value = [main_table]
        repo.get_ddl.return_value = related_ddl
        repo.get_all_flows.return_value = {}
        return repo

    def test_returns_empty_when_no_hints(self):
        repo = self._make_repo_with_hints([])
        intent = _intent()
        result = _fetch_related_tables(intent, repo)
        assert result == []

    def test_fetches_related_table_from_same_flow(self):
        hints = [{"to_table": "TbPessoa", "join": []}]
        related_ddl = {
            "schema": "dbo",
            "columns": [{"name": "IdPessoa", "type": "bigint", "nullable": False}],
            "constraints": {}
        }
        repo = self._make_repo_with_hints(hints, related_ddl=related_ddl)
        intent = _intent()
        result = _fetch_related_tables(intent, repo)
        assert len(result) == 1
        assert result[0]["name"] == "TbPessoa"

    def test_fetches_related_table_from_cross_flow(self):
        """Cobre o branch de busca em outros flows"""
        hints = [{"to_table": "TbPessoa", "join": []}]
        cross_ddl = {
            "schema": "dbo",
            "columns": [{"name": "IdPessoa", "type": "bigint", "nullable": False}],
            "constraints": {}
        }
        repo = MagicMock()
        
        # Mock get_tables_by_flow para retornar a tabela principal com hints
        main_table = {
            "table_profile": {
                "table_name": "TbProposta"
            },
            "relationships": {
                "outgoing": hints
            }
        }
        repo.get_tables_by_flow.return_value = [main_table]
        
        # Primeiro get_ddl retorna None (não encontrado no mesmo flow)
        # Segundo get_ddl retorna o DDL (encontrado em outro flow)
        repo.get_ddl.side_effect = [None, cross_ddl]
        
        # Mock get_all_flows para simular outros flows
        repo.get_all_flows.return_value = {
            "FLOW_A": {},
            "FLOW_B": {}
        }

        intent = _intent()
        result = _fetch_related_tables(intent, repo)
        assert len(result) == 1
        assert result[0]["name"] == "TbPessoa"
    
    def test_creates_minimal_entry_when_ddl_not_found(self):
        """Testa criação de entrada mínima quando DDL não é encontrado"""
        hints = [{
            "to_table": "TbLoja",
            "join": [{"right": "TbLoja.CdLoja"}]
        }]
        repo = self._make_repo_with_hints(hints, related_ddl=None)
        intent = _intent()
        result = _fetch_related_tables(intent, repo)
        assert len(result) == 1
        assert result[0]["name"] == "TbLoja"
        # Deve ter pelo menos a coluna de join
        col_names = [c["name"] for c in result[0]["columns"]]
        assert "CdLoja" in col_names

    def test_skips_duplicate_tables(self):
        hints = [
            {"to_table": "TbPessoa", "join": []},
            {"to_table": "TbPessoa", "join": []},  # duplicado
        ]
        related_ddl = {
            "schema": "dbo",
            "columns": [{"name": "IdPessoa", "type": "bigint", "nullable": False}],
            "constraints": {}
        }
        repo = self._make_repo_with_hints(hints, related_ddl=related_ddl)
        intent = _intent()
        result = _fetch_related_tables(intent, repo)
        assert len(result) == 1

    def test_returns_empty_when_doc_not_exists(self):
        repo = MagicMock()
        # Mock get_tables_by_flow para retornar lista vazia
        repo.get_tables_by_flow.return_value = []
        intent = _intent()
        result = _fetch_related_tables(intent, repo)
        assert result == []

    def test_handles_exception_gracefully(self):
        repo = MagicMock()
        repo.get_tables_by_flow.side_effect = Exception("Firestore error")
        intent = _intent()
        result = _fetch_related_tables(intent, repo)
        assert result == []
