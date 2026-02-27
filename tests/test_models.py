"""
Testes unitários — IntentObject, FilterCondition, DDLReference, enums
"""
import json
import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch
from src.models.intent import (
    IntentObject, FilterCondition, DDLReference, ValidationWarning,
    ValidationLevel, ProcessStatus
)


# ── fixtures ──────────────────────────────────────────────────────────────────

def _ddl_ref(flow_id="FLOW_A", table="TbProposta", schema="dbo"):
    return DDLReference(
        flow_id=flow_id,
        table_name=table,
        schema=schema,
        ddl_hash="abc123",
        columns_available=[
            {"name": "NuProposta", "type": "decimal(9,0)", "nullable": False},
            {"name": "StatusProposta", "type": "varchar(30)", "nullable": True},
        ],
        constraints={"primary_key": ["NuProposta"]},
        validated_at=datetime.now().isoformat()
    )


def _intent(filters=None, select_columns=None, warnings=None, status=ProcessStatus.SUCCESS):
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
        metadata={"schema": "dbo", "database_type": "SYBASE", "blocked_columns": []},
        ddl_reference=_ddl_ref(),
        sources_consulted={"flow_metadata": True},
        original_prompt="propostas aprovadas",
        created_at=datetime.now().isoformat(),
        status=status,
        warnings=warnings or []
    )


# ── enums ─────────────────────────────────────────────────────────────────────

class TestEnums:

    def test_validation_level_values(self):
        assert ValidationLevel.CRITICAL == "critical"
        assert ValidationLevel.WARNING == "warning"
        assert ValidationLevel.INFO == "info"

    def test_process_status_values(self):
        assert ProcessStatus.SUCCESS == "success"
        assert ProcessStatus.PARTIAL_SUCCESS == "partial_success"
        assert ProcessStatus.ERROR == "error"


# ── FilterCondition ───────────────────────────────────────────────────────────

class TestFilterCondition:

    def test_defaults(self):
        f = FilterCondition(
            column="StatusProposta", operator="=", value="Aprovada",
            nl_term="aprovada", resolved_via="test"
        )
        assert f.confidence == 1.0
        assert f.validated is True

    def test_custom_confidence(self):
        f = FilterCondition(
            column="Col", operator="=", value="X",
            nl_term="x", resolved_via="gemini", confidence=0.7
        )
        assert f.confidence == 0.7


# ── DDLReference ──────────────────────────────────────────────────────────────

class TestDDLReference:

    def test_fields(self):
        ref = _ddl_ref()
        assert ref.flow_id == "FLOW_A"
        assert ref.schema == "dbo"
        assert len(ref.columns_available) == 2
        assert ref.constraints["primary_key"] == ["NuProposta"]


# ── IntentObject.to_dict ──────────────────────────────────────────────────────

class TestIntentObjectToDict:

    def test_to_dict_basic(self):
        intent = _intent()
        d = intent.to_dict()
        assert d["flow_id"] == "FLOW_A"
        assert d["table_name"] == "TbProposta"
        assert d["status"] == "success"
        assert isinstance(d["filters"], list)
        assert isinstance(d["warnings"], list)

    def test_to_dict_with_filters(self):
        f = FilterCondition(
            column="StatusProposta", operator="=", value="Aprovada",
            nl_term="aprovada", resolved_via="test"
        )
        intent = _intent(filters=[f])
        d = intent.to_dict()
        assert len(d["filters"]) == 1
        assert d["filters"][0]["column"] == "StatusProposta"

    def test_to_dict_with_warnings(self):
        w = ValidationWarning(
            level=ValidationLevel.WARNING,
            category="column",
            message="Coluna não encontrada",
            details={},
            suggestions=[]
        )
        intent = _intent(warnings=[w], status=ProcessStatus.PARTIAL_SUCCESS)
        d = intent.to_dict()
        assert d["status"] == "partial_success"
        assert len(d["warnings"]) == 1
        assert d["warnings"][0]["level"] == "warning"

    def test_to_dict_status_is_string(self):
        intent = _intent(status=ProcessStatus.ERROR)
        d = intent.to_dict()
        assert isinstance(d["status"], str)
        assert d["status"] == "error"


# ── IntentObject.to_json ──────────────────────────────────────────────────────

class TestIntentObjectToJson:

    def test_to_json_is_valid_json(self):
        intent = _intent()
        json_str = intent.to_json()
        parsed = json.loads(json_str)
        assert parsed["flow_id"] == "FLOW_A"

    def test_to_json_indent(self):
        intent = _intent()
        json_str = intent.to_json(indent=4)
        assert "\n" in json_str  # indentado

    def test_to_json_unicode(self):
        intent = _intent()
        intent.original_prompt = "propostas com atraso"
        json_str = intent.to_json()
        assert "atraso" in json_str


# ── IntentObject.to_output ────────────────────────────────────────────────────

class TestIntentObjectToOutput:

    def test_to_output_without_repository(self):
        f = FilterCondition(
            column="StatusProposta", operator="=", value="Aprovada",
            nl_term="aprovada", resolved_via="test"
        )
        intent = _intent(filters=[f])
        output_str = intent.to_output(repository=None)
        output = json.loads(output_str)

        assert "parameters" in output
        assert "ddl" in output
        assert "filter_fields" in output["parameters"]
        assert "return_fields" in output["parameters"]

    def test_to_output_filter_fields_format(self):
        f = FilterCondition(
            column="StatusProposta", operator="=", value="Aprovada",
            nl_term="aprovada", resolved_via="test"
        )
        intent = _intent(filters=[f])
        output = json.loads(intent.to_output())
        ff = output["parameters"]["filter_fields"]
        assert len(ff) == 1
        key = list(ff[0].keys())[0]
        assert key == "dbo.TbProposta.StatusProposta"

    def test_to_output_return_fields_use_ddl_columns(self):
        intent = _intent()
        output = json.loads(intent.to_output())
        rf = output["parameters"]["return_fields"]
        assert "dbo.TbProposta.NuProposta" in rf
        assert "dbo.TbProposta.StatusProposta" in rf

    def test_to_output_ddl_structure(self):
        intent = _intent()
        output = json.loads(intent.to_output())
        ddl = output["ddl"]
        assert "tables" in ddl
        assert len(ddl["tables"]) >= 1
        table = ddl["tables"][0]
        assert table["name"] == "TbProposta"
        assert table["schema"] == "dbo"
        assert "columns" in table

    def test_to_output_primary_key_in_ddl(self):
        intent = _intent()
        output = json.loads(intent.to_output())
        table = output["ddl"]["tables"][0]
        assert "primaryKey" in table
        assert "NuProposta" in table["primaryKey"]
