"""
Testes unitários — ValidationService
"""
import pytest
from unittest.mock import MagicMock
from src.services.validator import ValidationService
from src.models.intent import (
    FilterCondition, ValidationLevel, ProcessStatus, ValidationWarning
)


# ── fixtures ──────────────────────────────────────────────────────────────────

def _make_repo(flow_exists=True, table_exists=True):
    repo = MagicMock()
    repo.get_flow.return_value = {"flow_id": "FLOW_A"} if flow_exists else None
    repo.get_table.return_value = {"table_profile": {"table_name": "TbProposta"}} if table_exists else None
    repo.get_all_flows.return_value = {"FLOW_A": {}, "FLOW_B": {}}
    repo.get_tables_by_flow.return_value = [
        {"table_profile": {"table_name": "TbProposta"}},
        {"table_profile": {"table_name": "TbPessoa"}},
    ]
    return repo


DDL_COLUMNS = [
    {"name": "NuProposta", "type": "decimal(9,0)", "nullable": False},
    {"name": "StatusProposta", "type": "varchar(30)", "nullable": True},
    {"name": "DtCriacao", "type": "datetime", "nullable": True},
    {"name": "CdProduto", "type": "int", "nullable": False},
]


def _filter(column, operator="=", value="X"):
    return FilterCondition(
        column=column, operator=operator, value=value,
        nl_term=column, resolved_via="test"
    )


# ── validate_flow_and_table ───────────────────────────────────────────────────

class TestValidateFlowAndTable:

    def test_flow_and_table_exist(self):
        svc = ValidationService(_make_repo())
        ok, warnings = svc.validate_flow_and_table("FLOW_A", "TbProposta")
        assert ok is True
        assert warnings == []

    def test_flow_not_found_returns_critical(self):
        svc = ValidationService(_make_repo(flow_exists=False))
        ok, warnings = svc.validate_flow_and_table("FLOW_X", "TbProposta")
        assert ok is False
        assert len(warnings) == 1
        assert warnings[0].level == ValidationLevel.CRITICAL
        assert warnings[0].category == "flow"
        assert "FLOW_X" in warnings[0].message

    def test_table_not_found_returns_critical(self):
        svc = ValidationService(_make_repo(table_exists=False))
        ok, warnings = svc.validate_flow_and_table("FLOW_A", "TbInexistente")
        assert ok is False
        assert len(warnings) == 1
        assert warnings[0].level == ValidationLevel.CRITICAL
        assert warnings[0].category == "table"

    def test_flow_not_found_includes_suggestions(self):
        repo = _make_repo(flow_exists=False)
        repo.get_all_flows.return_value = {"FLOW_PROPOSTA": {}, "FLOW_VEICULO": {}}
        svc = ValidationService(repo)
        ok, warnings = svc.validate_flow_and_table("FLOW_PROPOST", "TbProposta")
        assert ok is False
        # deve sugerir FLOW_PROPOSTA por similaridade
        assert "FLOW_PROPOSTA" in warnings[0].suggestions


# ── validate_columns ──────────────────────────────────────────────────────────

class TestValidateColumns:

    def test_valid_columns_pass_through(self):
        svc = ValidationService(_make_repo())
        filters = [_filter("NuProposta"), _filter("StatusProposta")]
        validated, warnings = svc.validate_columns(filters, DDL_COLUMNS)
        assert len(validated) == 2
        assert warnings == []

    def test_invalid_column_generates_warning(self):
        svc = ValidationService(_make_repo())
        filters = [_filter("ColunaInexistente")]
        validated, warnings = svc.validate_columns(filters, DDL_COLUMNS)
        assert validated == []
        assert len(warnings) == 1
        assert warnings[0].level == ValidationLevel.WARNING
        assert "ColunaInexistente" in warnings[0].message

    def test_mixed_columns_keeps_valid_only(self):
        svc = ValidationService(_make_repo())
        filters = [_filter("NuProposta"), _filter("ColInvalida")]
        validated, warnings = svc.validate_columns(filters, DDL_COLUMNS)
        assert len(validated) == 1
        assert validated[0].column == "NuProposta"
        assert len(warnings) == 1

    def test_case_insensitive_match(self):
        svc = ValidationService(_make_repo())
        filters = [_filter("nuproposta")]
        validated, warnings = svc.validate_columns(filters, DDL_COLUMNS)
        assert len(validated) == 1
        assert warnings == []

    def test_empty_filters_returns_empty(self):
        svc = ValidationService(_make_repo())
        validated, warnings = svc.validate_columns([], DDL_COLUMNS)
        assert validated == []
        assert warnings == []


# ── validate_select_columns ───────────────────────────────────────────────────

class TestValidateSelectColumns:

    def test_valid_select_columns(self):
        svc = ValidationService(_make_repo())
        validated, warnings = svc.validate_select_columns(
            ["NuProposta", "CdProduto"], DDL_COLUMNS
        )
        assert set(validated) == {"NuProposta", "CdProduto"}
        assert warnings == []

    def test_invalid_select_column_generates_warning(self):
        svc = ValidationService(_make_repo())
        validated, warnings = svc.validate_select_columns(
            ["ColInexistente"], DDL_COLUMNS
        )
        assert validated == []
        assert len(warnings) == 1
        assert warnings[0].level == ValidationLevel.WARNING

    def test_case_insensitive_select(self):
        svc = ValidationService(_make_repo())
        validated, warnings = svc.validate_select_columns(
            ["statusproposta"], DDL_COLUMNS
        )
        assert len(validated) == 1
        assert warnings == []


# ── calculate_status_and_confidence ──────────────────────────────────────────

class TestCalculateStatusAndConfidence:

    def _warning(self, level):
        return ValidationWarning(
            level=level, category="test",
            message="msg", details={}, suggestions=[]
        )

    def test_no_warnings_returns_success(self):
        svc = ValidationService(_make_repo())
        status, conf = svc.calculate_status_and_confidence([], 0.9)
        assert status == ProcessStatus.SUCCESS
        assert conf == 0.9

    def test_critical_warning_returns_error(self):
        svc = ValidationService(_make_repo())
        status, conf = svc.calculate_status_and_confidence(
            [self._warning(ValidationLevel.CRITICAL)], 0.9
        )
        assert status == ProcessStatus.ERROR
        assert conf == 0.0

    def test_warning_returns_partial_success(self):
        svc = ValidationService(_make_repo())
        status, conf = svc.calculate_status_and_confidence(
            [self._warning(ValidationLevel.WARNING)], 0.9
        )
        assert status == ProcessStatus.PARTIAL_SUCCESS
        assert conf < 0.9

    def test_multiple_warnings_reduce_confidence(self):
        svc = ValidationService(_make_repo())
        warnings = [self._warning(ValidationLevel.WARNING)] * 3
        status, conf = svc.calculate_status_and_confidence(warnings, 0.9)
        assert status == ProcessStatus.PARTIAL_SUCCESS
        assert round(conf, 10) <= 0.6

    def test_confidence_floor_at_0_3(self):
        svc = ValidationService(_make_repo())
        warnings = [self._warning(ValidationLevel.WARNING)] * 10
        _, conf = svc.calculate_status_and_confidence(warnings, 0.4)
        assert conf >= 0.3


# ── _fuzzy_match ──────────────────────────────────────────────────────────────

class TestFuzzyMatch:

    def test_exact_match(self):
        svc = ValidationService(_make_repo())
        result = svc._fuzzy_match("FLOW_A", ["FLOW_A", "FLOW_B"])
        assert "FLOW_A" in result

    def test_similar_match(self):
        svc = ValidationService(_make_repo())
        result = svc._fuzzy_match("FLOW_PROPOST", ["FLOW_PROPOSTA", "FLOW_VEICULO"])
        assert "FLOW_PROPOSTA" in result

    def test_no_match_below_threshold(self):
        svc = ValidationService(_make_repo())
        result = svc._fuzzy_match("XYZ", ["FLOW_PROPOSTA", "FLOW_VEICULO"])
        assert result == []

    def test_empty_candidates(self):
        svc = ValidationService(_make_repo())
        result = svc._fuzzy_match("FLOW_A", [])
        assert result == []

    def test_skips_empty_string_candidates(self):
        """Cobre linha 208: candidate falsy é ignorado"""
        svc = ValidationService(_make_repo())
        result = svc._fuzzy_match("FLOW_A", ["", None, "FLOW_A"])
        assert "FLOW_A" in result
