"""
Testes unitários — IntentAgent (todos os branches com mocks)
"""
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime
from src.agent.intent_agent import IntentAgent
from src.models.intent import (
    IntentObject, FilterCondition, DDLReference,
    ValidationWarning, ValidationLevel, ProcessStatus
)


# ── fixtures ──────────────────────────────────────────────────────────────────

DDL_DATA = {
    "schema": "dbo",
    "columns": [
        {"name": "NuProposta",     "type": "decimal(9,0)", "nullable": False},
        {"name": "StatusProposta", "type": "varchar(30)",  "nullable": True},
    ],
    "constraints": {"primary_key": ["NuProposta"], "foreign_keys": []}
}


def _ddl_ref():
    return DDLReference(
        flow_id="FLOW_A", table_name="TbProposta", schema="dbo",
        ddl_hash="abc123", columns_available=DDL_DATA["columns"],
        constraints=DDL_DATA["constraints"],
        validated_at=datetime.now().isoformat()
    )


def _make_intent(filters=None, select_columns=None, status=ProcessStatus.SUCCESS):
    return IntentObject(
        flow_id="FLOW_A", table_name="TbProposta",
        intent_type="massa_para_teste",
        filters=filters or [], select_columns=select_columns or [],
        joins=[], order_by=[], limit=3, confidence_score=0.85,
        metadata={"schema": "dbo", "database_type": "SYBASE", "blocked_columns": []},
        ddl_reference=_ddl_ref(), sources_consulted={},
        original_prompt="test", created_at=datetime.now().isoformat(),
        status=status, warnings=[]
    )


def _make_agent(
    flow_exists=True, table_exists=True,
    ddl_data=None, strategy_intent=None,
    validated_filters=None, column_warnings=None,
    select_warnings=None, candidate_columns=2
):
    repo = MagicMock()
    repo.get_ddl.return_value = ddl_data or DDL_DATA

    # validator mock
    validator = MagicMock()
    validator.validate_flow_and_table.return_value = (
        flow_exists and table_exists,
        [] if (flow_exists and table_exists) else [
            ValidationWarning(
                level=ValidationLevel.CRITICAL, category="flow",
                message="Flow não encontrado", details={}, suggestions=[]
            )
        ]
    )
    validator.validate_columns.return_value = (
        validated_filters if validated_filters is not None else [],
        column_warnings or []
    )
    validator.validate_select_columns.return_value = ([], select_warnings or [])
    validator.calculate_status_and_confidence.return_value = (ProcessStatus.SUCCESS, 0.85)

    # strategies
    local_strategy = MagicMock()
    ai_strategy = MagicMock()
    intent = strategy_intent or _make_intent()
    local_strategy.build_intent.return_value = intent
    ai_strategy.build_intent.return_value = intent

    # filter extractor mock (para _select_strategy)
    fe_mock = MagicMock()
    fe_mock.get_candidate_columns.return_value = [MagicMock()] * candidate_columns

    agent = IntentAgent(repo, local_strategy, ai_strategy, gemini_threshold=0.5)
    agent.validator = validator

    return agent, local_strategy, ai_strategy, fe_mock


# ── STEP 1: validação crítica ─────────────────────────────────────────────────

class TestStep1Validation:

    def test_returns_error_intent_when_flow_not_found(self):
        agent, _, _, _ = _make_agent(flow_exists=False)
        result = agent.process("test", "FLOW_X", "TbProposta")
        assert result.status == ProcessStatus.ERROR
        assert result.flow_id == "FLOW_X"

    def test_returns_error_intent_when_table_not_found(self):
        agent, _, _, _ = _make_agent(table_exists=False)
        result = agent.process("test", "FLOW_A", "TbInexistente")
        assert result.status == ProcessStatus.ERROR

    def test_error_intent_has_zero_confidence(self):
        agent, _, _, _ = _make_agent(flow_exists=False)
        result = agent.process("test", "FLOW_X", "TbProposta")
        assert result.confidence_score == 0.0

    def test_error_intent_has_warnings(self):
        agent, _, _, _ = _make_agent(flow_exists=False)
        result = agent.process("test", "FLOW_X", "TbProposta")
        assert len(result.warnings) > 0


# ── STEP 2: DDL não encontrado ────────────────────────────────────────────────

class TestStep2DDL:

    def test_raises_when_ddl_not_found(self):
        agent, _, _, _ = _make_agent(ddl_data=None)
        agent.repo.get_ddl.return_value = None
        with pytest.raises(ValueError, match="DDL não encontrado"):
            agent.process("test", "FLOW_A", "TbProposta")


# ── STEP 4: seleção de estratégia ─────────────────────────────────────────────

class TestStep4StrategySelection:

    def test_uses_local_strategy_when_score_high(self):
        agent, local, ai, fe = _make_agent(candidate_columns=5)
        with patch("src.services.filter_extractor.FilterExtractor") as MockFE:
            MockFE.return_value = fe
            agent.process("test", "FLOW_A", "TbProposta", flow_score=8.0)
        local.build_intent.assert_called_once()
        ai.build_intent.assert_not_called()

    def test_uses_ai_strategy_when_score_low(self):
        agent, local, ai, fe = _make_agent(candidate_columns=5)
        with patch("src.services.filter_extractor.FilterExtractor") as MockFE:
            MockFE.return_value = fe
            agent.process("test", "FLOW_A", "TbProposta", flow_score=1.0)
        ai.build_intent.assert_called_once()
        local.build_intent.assert_not_called()

    def test_uses_ai_strategy_when_few_columns(self):
        agent, local, ai, fe = _make_agent(candidate_columns=1)
        with patch("src.services.filter_extractor.FilterExtractor") as MockFE:
            MockFE.return_value = fe
            agent.process("test", "FLOW_A", "TbProposta", flow_score=8.0)
        ai.build_intent.assert_called_once()

    def test_default_score_is_10_uses_local(self):
        agent, local, ai, fe = _make_agent(candidate_columns=5)
        with patch("src.services.filter_extractor.FilterExtractor") as MockFE:
            MockFE.return_value = fe
            agent.process("test", "FLOW_A", "TbProposta")  # sem flow_score
        local.build_intent.assert_called_once()


# ── STEP 6 & 7: validação de colunas e status final ──────────────────────────

class TestStep6And7:

    def test_validated_filters_applied_to_intent(self):
        f = FilterCondition(
            column="StatusProposta", operator="=", value="Aprovada",
            nl_term="aprovada", resolved_via="test"
        )
        agent, local, ai, fe = _make_agent(validated_filters=[f])
        agent.validator.calculate_status_and_confidence.return_value = (ProcessStatus.SUCCESS, 0.85)
        with patch("src.services.filter_extractor.FilterExtractor") as MockFE:
            MockFE.return_value = fe
            result = agent.process("test", "FLOW_A", "TbProposta", flow_score=8.0)
        assert len(result.filters) == 1

    def test_column_warnings_added_to_intent(self):
        w = ValidationWarning(
            level=ValidationLevel.WARNING, category="column",
            message="Coluna não encontrada", details={}, suggestions=[]
        )
        agent, local, ai, fe = _make_agent(column_warnings=[w])
        agent.validator.calculate_status_and_confidence.return_value = (
            ProcessStatus.PARTIAL_SUCCESS, 0.75
        )
        with patch("src.services.filter_extractor.FilterExtractor") as MockFE:
            MockFE.return_value = fe
            result = agent.process("test", "FLOW_A", "TbProposta", flow_score=8.0)
        assert len(result.warnings) >= 1

    def test_select_columns_validated_when_present(self):
        intent_with_select = _make_intent(select_columns=["StatusProposta"])
        agent, local, ai, fe = _make_agent(strategy_intent=intent_with_select)
        agent.validator.calculate_status_and_confidence.return_value = (ProcessStatus.SUCCESS, 0.85)
        with patch("src.services.filter_extractor.FilterExtractor") as MockFE:
            MockFE.return_value = fe
            agent.process("test", "FLOW_A", "TbProposta", flow_score=8.0)
        agent.validator.validate_select_columns.assert_called_once()

    def test_final_status_set_on_intent(self):
        agent, local, ai, fe = _make_agent()
        agent.validator.calculate_status_and_confidence.return_value = (
            ProcessStatus.PARTIAL_SUCCESS, 0.70
        )
        with patch("src.services.filter_extractor.FilterExtractor") as MockFE:
            MockFE.return_value = fe
            result = agent.process("test", "FLOW_A", "TbProposta", flow_score=8.0)
        assert result.status == ProcessStatus.PARTIAL_SUCCESS
        assert result.confidence_score == 0.70


# ── _create_error_intent ──────────────────────────────────────────────────────

class TestCreateErrorIntent:

    def test_error_intent_fields(self):
        agent, _, _, _ = _make_agent()
        w = ValidationWarning(
            level=ValidationLevel.CRITICAL, category="flow",
            message="Flow não encontrado", details={}, suggestions=[]
        )
        result = agent._create_error_intent("meu prompt", "FLOW_X", "TbX", [w])
        assert result.status == ProcessStatus.ERROR
        assert result.flow_id == "FLOW_X"
        assert result.table_name == "TbX"
        assert result.original_prompt == "meu prompt"
        assert result.confidence_score == 0.0
        assert result.intent_type == "error"
        assert len(result.warnings) == 1
