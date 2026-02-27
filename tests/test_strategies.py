"""
Testes unitários — AIStrategy e LocalStrategy
"""
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime
from src.strategies.ai_strategy import AIStrategy
from src.strategies.local_strategy import LocalStrategy
from src.models.intent import IntentObject, FilterCondition, ProcessStatus


# ── fixtures ──────────────────────────────────────────────────────────────────

DDL_DATA = {
    "schema": "dbo",
    "columns": [
        {"name": "NuProposta",     "type": "decimal(9,0)", "nullable": False},
        {"name": "StatusProposta", "type": "varchar(30)",  "nullable": True},
        {"name": "DtCriacao",      "type": "datetime",     "nullable": True},
    ],
    "constraints": {
        "primary_key": ["NuProposta"],
        "foreign_keys": []
    }
}

FLOW_DATA = {
    "flow_id": "FLOW_A",
    "_match_score": 10,
    "semantic_profile": {"description": "Propostas de veículos"},
    "aliases": {"seed": []},
    "return_expected": {
        "purpose": "massa_para_teste",
        "blocked_columns": [],
        "sorting_preference": [],
        "limit_default": 3
    },
    "entities": {"grain_keys": ["NuProposta"]},
    "database": {"type": "SYBASE", "dialect": "tsql_sybase"}
}

TABLE_DATA = {
    "table_profile": {
        "table_name": "TbProposta",
        "description": "Tabela de propostas",
        "grain": {"grain_keys": ["NuProposta"]}
    },
    "columns_dictionary": [
        {
            "name": "StatusProposta",
            "description": "Status da proposta",
            "ai_hints": {
                "filter_candidate": True,
                "nl_terms_seed": ["status", "aprovada"],
                "recommended_ops": ["="]
            }
        }
    ]
}


# ── AIStrategy ────────────────────────────────────────────────────────────────

class TestAIStrategy:

    def _make_ai_service(self, filters=None, confidence=0.85):
        svc = MagicMock()
        svc.infer_intent.return_value = {
            "filters": filters or [
                {"column": "StatusProposta", "operator": "=", "value": "Aprovada",
                 "nl_term": "aprovada", "confidence": 0.9}
            ],
            "select_columns": [],
            "order_by": [],
            "limit": 3,
            "confidence_score": confidence,
            "reasoning": "Detectou status aprovada"
        }
        return svc

    def test_build_intent_returns_intent_object(self):
        strategy = AIStrategy(self._make_ai_service())
        intent = strategy.build_intent("propostas aprovadas", FLOW_DATA, TABLE_DATA, DDL_DATA)
        assert isinstance(intent, IntentObject)

    def test_build_intent_flow_id(self):
        strategy = AIStrategy(self._make_ai_service())
        intent = strategy.build_intent("propostas aprovadas", FLOW_DATA, TABLE_DATA, DDL_DATA)
        assert intent.flow_id == "FLOW_A"

    def test_build_intent_table_name(self):
        strategy = AIStrategy(self._make_ai_service())
        intent = strategy.build_intent("propostas aprovadas", FLOW_DATA, TABLE_DATA, DDL_DATA)
        assert intent.table_name == "TbProposta"

    def test_build_intent_filters_from_gemini(self):
        strategy = AIStrategy(self._make_ai_service())
        intent = strategy.build_intent("propostas aprovadas", FLOW_DATA, TABLE_DATA, DDL_DATA)
        assert len(intent.filters) == 1
        assert intent.filters[0].column == "StatusProposta"
        assert intent.filters[0].resolved_via == "gemini_inference"

    def test_build_intent_confidence(self):
        strategy = AIStrategy(self._make_ai_service(confidence=0.92))
        intent = strategy.build_intent("propostas aprovadas", FLOW_DATA, TABLE_DATA, DDL_DATA)
        assert intent.confidence_score == 0.92

    def test_build_intent_ddl_reference(self):
        strategy = AIStrategy(self._make_ai_service())
        intent = strategy.build_intent("propostas aprovadas", FLOW_DATA, TABLE_DATA, DDL_DATA)
        assert intent.ddl_reference.schema == "dbo"
        assert intent.ddl_reference.flow_id == "FLOW_A"

    def test_build_intent_sources_consulted_includes_gemini(self):
        strategy = AIStrategy(self._make_ai_service())
        intent = strategy.build_intent("propostas aprovadas", FLOW_DATA, TABLE_DATA, DDL_DATA)
        assert intent.sources_consulted.get("gemini") is True

    def test_build_context_structure(self):
        strategy = AIStrategy(MagicMock())
        ctx = strategy._build_context(FLOW_DATA, TABLE_DATA, DDL_DATA)
        assert "flow" in ctx
        assert "table" in ctx
        assert "columns" in ctx
        assert "ddl_columns" in ctx

    def test_build_context_only_filter_candidates(self):
        strategy = AIStrategy(MagicMock())
        ctx = strategy._build_context(FLOW_DATA, TABLE_DATA, DDL_DATA)
        # TABLE_DATA tem 1 coluna com filter_candidate=True
        assert len(ctx["columns"]) == 1

    def test_create_ddl_reference_hash(self):
        strategy = AIStrategy(MagicMock())
        ref = strategy._create_ddl_reference("FLOW_A", "TbProposta", DDL_DATA)
        assert len(ref.ddl_hash) == 16

    def test_build_metadata_keys(self):
        strategy = AIStrategy(MagicMock())
        ref = strategy._create_ddl_reference("FLOW_A", "TbProposta", DDL_DATA)
        meta = strategy._build_metadata(FLOW_DATA, ref, {"reasoning": "ok"})
        assert "schema" in meta
        assert "database_type" in meta
        assert "gemini_reasoning" in meta


# ── LocalStrategy ─────────────────────────────────────────────────────────────

class TestLocalStrategy:

    _SENTINEL = object()

    def _make_filter_extractor(self, filters=_SENTINEL):
        fe = MagicMock()
        fe.get_candidate_columns.return_value = [
            {"name": "StatusProposta", "ai_hints": {"nl_terms_seed": ["status"]}}
        ]
        fe.extract_from_columns.return_value = (
            filters if filters is not self._SENTINEL else [
                FilterCondition(
                    column="StatusProposta", operator="=", value="Aprovada",
                    nl_term="aprovada", resolved_via="heuristic_status"
                )
            ]
        )
        return fe

    def test_build_intent_returns_intent_object(self):
        strategy = LocalStrategy(self._make_filter_extractor())
        intent = strategy.build_intent("propostas aprovadas", FLOW_DATA, TABLE_DATA, DDL_DATA)
        assert isinstance(intent, IntentObject)

    def test_build_intent_flow_id(self):
        strategy = LocalStrategy(self._make_filter_extractor())
        intent = strategy.build_intent("propostas aprovadas", FLOW_DATA, TABLE_DATA, DDL_DATA)
        assert intent.flow_id == "FLOW_A"

    def test_build_intent_filters(self):
        strategy = LocalStrategy(self._make_filter_extractor())
        intent = strategy.build_intent("propostas aprovadas", FLOW_DATA, TABLE_DATA, DDL_DATA)
        assert len(intent.filters) == 1
        assert intent.filters[0].column == "StatusProposta"

    def test_build_intent_confidence_is_0_75(self):
        strategy = LocalStrategy(self._make_filter_extractor())
        intent = strategy.build_intent("propostas aprovadas", FLOW_DATA, TABLE_DATA, DDL_DATA)
        assert intent.confidence_score == 0.75

    def test_build_intent_strategy_in_sources(self):
        strategy = LocalStrategy(self._make_filter_extractor())
        intent = strategy.build_intent("propostas aprovadas", FLOW_DATA, TABLE_DATA, DDL_DATA)
        assert intent.sources_consulted.get("strategy") == "local"

    def test_build_intent_no_filters(self):
        fe = self._make_filter_extractor(filters=[])
        strategy = LocalStrategy(fe)
        intent = strategy.build_intent("propostas", FLOW_DATA, TABLE_DATA, DDL_DATA)
        assert intent.filters == []

    def test_create_ddl_reference(self):
        strategy = LocalStrategy(MagicMock())
        ref = strategy._create_ddl_reference("FLOW_A", "TbProposta", DDL_DATA)
        assert ref.schema == "dbo"
        assert len(ref.ddl_hash) == 16

    def test_build_metadata_keys(self):
        strategy = LocalStrategy(MagicMock())
        ref = strategy._create_ddl_reference("FLOW_A", "TbProposta", DDL_DATA)
        meta = strategy._build_metadata(FLOW_DATA, ref)
        assert "schema" in meta
        assert "database_type" in meta
        assert "grain_keys" in meta


# ── IntentStrategy base ───────────────────────────────────────────────────────

class TestIntentStrategyBase:

    def test_abstract_method_covered_via_concrete(self):
        """Garante que o método abstrato é coberto via implementação concreta"""
        from src.strategies.base import IntentStrategy
        with pytest.raises(TypeError):
            IntentStrategy()

    def test_abstract_pass_via_super(self):
        """Cobre linha 30: chama super().build_intent() para executar o pass"""
        from src.strategies.base import IntentStrategy

        class ConcreteStrategy(IntentStrategy):
            def build_intent(self, user_prompt, flow_data, table_data, ddl_data):
                super().build_intent(user_prompt, flow_data, table_data, ddl_data)
                return MagicMock()

        strategy = ConcreteStrategy()
        result = strategy.build_intent("test", {}, {}, {})
        assert result is not None
