"""
Testes unitários — FilterExtractor
"""
import pytest
from unittest.mock import MagicMock
from src.services.filter_extractor import FilterExtractor
from src.models.intent import FilterCondition


# ── fixtures ──────────────────────────────────────────────────────────────────

DDL_COLUMNS = [
    {"name": "DtCriacao",      "type": "datetime",    "nullable": True},
    {"name": "StatusProposta", "type": "varchar(30)", "nullable": True},
    {"name": "CdProduto",      "type": "int",         "nullable": False},
    {"name": "DsProduto",      "type": "varchar(100)","nullable": True},
]

FLOW_DATA = {
    "aliases": {
        "seed": [
            {
                "canonical": "CDC",
                "variants": ["cdc", "credito direto ao consumidor"],
                "resolved_value": "CDC"
            },
            {
                "canonical": "Aprovada",
                "variants": ["aprovada", "aprovado"],
                "resolved_value": "Aprovada"
            }
        ]
    }
}

COLUMNS_DICT = [
    {
        "name": "DtCriacao",
        "description": "Data de criação da proposta",
        "ai_hints": {
            "filter_candidate": True,
            "nl_terms_seed": ["data", "criação", "criacao"],
            "recommended_ops": [">=", "<="]
        }
    },
    {
        "name": "StatusProposta",
        "description": "Status atual da proposta",
        "ai_hints": {
            "filter_candidate": True,
            "nl_terms_seed": ["status", "situação"],
            "recommended_ops": ["=", "IN"]
        }
    },
    {
        "name": "CdProduto",
        "description": "Código do produto",
        "ai_hints": {
            "filter_candidate": True,
            "nl_terms_seed": ["produto", "cdc"],
            "recommended_ops": ["="]
        }
    },
    {
        "name": "NuProposta",
        "description": "Número da proposta",
        "ai_hints": {
            "filter_candidate": False,  # não é candidato
            "nl_terms_seed": [],
        }
    }
]


def _make_repo(columns_dict=None):
    repo = MagicMock()
    repo.get_table.return_value = {
        "table_profile": {"table_name": "TbProposta"},
        "columns_dictionary": columns_dict or COLUMNS_DICT
    }
    return repo


# ── get_candidate_columns ─────────────────────────────────────────────────────

class TestGetCandidateColumns:

    def test_returns_only_filter_candidates(self):
        fe = FilterExtractor(_make_repo())
        cols = fe.get_candidate_columns("FLOW_A", "TbProposta", "propostas aprovadas")
        names = [c["name"] for c in cols]
        assert "NuProposta" not in names  # filter_candidate=False

    def test_scores_by_nl_term_match(self):
        fe = FilterExtractor(_make_repo())
        cols = fe.get_candidate_columns("FLOW_A", "TbProposta", "status aprovada")
        names = [c["name"] for c in cols]
        assert "StatusProposta" in names

    def test_returns_empty_when_table_not_found(self):
        repo = MagicMock()
        repo.get_table.return_value = None
        fe = FilterExtractor(repo)
        cols = fe.get_candidate_columns("FLOW_X", "TbInexistente", "qualquer coisa")
        assert cols == []

    def test_max_10_candidates(self):
        # 15 colunas candidatas
        many_cols = [
            {"name": f"Col{i}", "description": "x",
             "ai_hints": {"filter_candidate": True, "nl_terms_seed": [f"col{i}"]}}
            for i in range(15)
        ]
        fe = FilterExtractor(_make_repo(many_cols))
        cols = fe.get_candidate_columns("FLOW_A", "TbProposta", " ".join(f"col{i}" for i in range(15)))
        assert len(cols) <= 10


# ── _extract_temporal_filter ──────────────────────────────────────────────────

class TestExtractTemporalFilter:

    def test_extracts_days_filter(self):
        fe = FilterExtractor(MagicMock())
        result = fe._extract_temporal_filter("propostas dos últimos 30 dias", "DtCriacao")
        assert result is not None
        assert result.column == "DtCriacao"
        assert result.operator == ">="
        assert "30" in result.value

    def test_no_temporal_term_returns_none(self):
        fe = FilterExtractor(MagicMock())
        result = fe._extract_temporal_filter("propostas aprovadas", "DtCriacao")
        assert result is None

    def test_temporal_term_without_number_returns_none(self):
        """Cobre o branch: tem termo temporal mas sem número de dias"""
        fe = FilterExtractor(MagicMock())
        result = fe._extract_temporal_filter("propostas do último mês", "DtCriacao")
        assert result is None

    def test_temporal_with_semanas_term_no_number_returns_none(self):
        fe = FilterExtractor(MagicMock())
        result = fe._extract_temporal_filter("propostas das últimas semanas", "DtCriacao")
        assert result is None

    def test_temporal_with_number(self):
        fe = FilterExtractor(MagicMock())
        result = fe._extract_temporal_filter("últimos 60 dias", "DtCriacao")
        assert result is not None
        assert "60" in result.value
        assert result.resolved_via == "heuristic_temporal"


# ── _extract_alias_filter ─────────────────────────────────────────────────────

class TestExtractAliasFilter:

    def test_extracts_alias_by_canonical(self):
        fe = FilterExtractor(MagicMock())
        col = {"name": "CdProduto", "ai_hints": {"nl_terms_seed": ["CDC"]}}
        result = fe._extract_alias_filter("propostas cdc", col, FLOW_DATA)
        assert result is not None
        assert result.column == "CdProduto"
        assert result.value == "CDC"
        assert result.resolved_via == "alias_match"

    def test_extracts_alias_by_variant(self):
        fe = FilterExtractor(MagicMock())
        col = {"name": "CdProduto", "ai_hints": {"nl_terms_seed": ["credito direto ao consumidor"]}}
        result = fe._extract_alias_filter("credito direto ao consumidor", col, FLOW_DATA)
        assert result is not None

    def test_term_in_prompt_but_no_alias_returns_none(self):
        """Cobre linha 155: termo encontrado no prompt mas sem alias no flow_data"""
        fe = FilterExtractor(MagicMock())
        col = {"name": "CdProduto", "ai_hints": {"nl_terms_seed": ["produto"]}}
        # "produto" está no prompt mas não existe alias para ele no flow_data
        result = fe._extract_alias_filter("propostas de produto", col, FLOW_DATA)
        assert result is None


# ── _extract_status_filter ────────────────────────────────────────────────────

class TestExtractStatusFilter:

    def test_extracts_aprovada(self):
        fe = FilterExtractor(MagicMock())
        result = fe._extract_status_filter("propostas aprovadas", "StatusProposta")
        assert result is not None
        assert result.value == "Aprovada"
        assert result.resolved_via == "heuristic_status"

    def test_extracts_recusada(self):
        fe = FilterExtractor(MagicMock())
        result = fe._extract_status_filter("propostas recusadas", "StatusProposta")
        assert result is not None
        assert result.value == "Recusada"

    def test_no_status_returns_none(self):
        fe = FilterExtractor(MagicMock())
        result = fe._extract_status_filter("propostas dos últimos 30 dias", "StatusProposta")
        assert result is None


# ── _search_alias ─────────────────────────────────────────────────────────────

class TestSearchAlias:

    def test_finds_by_canonical(self):
        fe = FilterExtractor(MagicMock())
        result = fe._search_alias("CDC", FLOW_DATA)
        assert result is not None
        assert result["canonical"] == "CDC"

    def test_finds_by_variant(self):
        fe = FilterExtractor(MagicMock())
        result = fe._search_alias("aprovada", FLOW_DATA)
        assert result is not None

    def test_not_found_returns_none(self):
        fe = FilterExtractor(MagicMock())
        result = fe._search_alias("inexistente", FLOW_DATA)
        assert result is None

    def test_empty_flow_data(self):
        fe = FilterExtractor(MagicMock())
        result = fe._search_alias("CDC", {})
        assert result is None


# ── extract_from_columns ──────────────────────────────────────────────────────

class TestExtractFromColumns:

    def test_extracts_temporal_from_datetime_column(self):
        fe = FilterExtractor(MagicMock())
        col = {
            "name": "DtCriacao",
            "ai_hints": {"nl_terms_seed": ["data"]}
        }
        filters = fe.extract_from_columns(
            "propostas dos últimos 30 dias",
            [col], DDL_COLUMNS, FLOW_DATA
        )
        assert any(f.column == "DtCriacao" for f in filters)

    def test_extracts_temporal_via_heuristic_full_path(self):
        """Cobre linhas 128, 132-134: _extract_filter_heuristic → _extract_temporal_filter com match"""
        fe = FilterExtractor(MagicMock())
        col = {"name": "DtCriacao", "ai_hints": {"nl_terms_seed": []}}
        ddl_col = {"name": "DtCriacao", "type": "datetime", "nullable": True}
        result = fe._extract_filter_heuristic("últimos 45 dias", col, [ddl_col], FLOW_DATA)
        assert result is not None
        assert result.column == "DtCriacao"
        assert "45" in result.value

    def test_skips_column_not_in_ddl(self):
        fe = FilterExtractor(MagicMock())
        col = {
            "name": "ColInexistente",
            "ai_hints": {"nl_terms_seed": ["inexistente"]}
        }
        filters = fe.extract_from_columns(
            "propostas dos últimos 30 dias",
            [col], DDL_COLUMNS, FLOW_DATA
        )
        assert filters == []

    def test_returns_empty_for_no_columns(self):
        fe = FilterExtractor(MagicMock())
        filters = fe.extract_from_columns("qualquer coisa", [], DDL_COLUMNS, FLOW_DATA)
        assert filters == []

    def test_heuristic_returns_none_when_no_match(self):
        """Cobre linha 164: _extract_filter_heuristic retorna None quando nenhuma heurística bate"""
        fe = FilterExtractor(MagicMock())
        # Coluna não é datetime, não tem alias, não tem 'status' no nome
        col = {"name": "CdProduto", "ai_hints": {"nl_terms_seed": ["xyz_inexistente"]}}
        ddl_col = {"name": "CdProduto", "type": "int", "nullable": False}
        result = fe._extract_filter_heuristic("propostas normais", col, [ddl_col], FLOW_DATA)
        assert result is None

    def test_heuristic_returns_alias_filter(self):
        """Cobre linha 128: _extract_filter_heuristic retorna alias_filter"""
        fe = FilterExtractor(MagicMock())
        col = {"name": "CdProduto", "ai_hints": {"nl_terms_seed": ["CDC"]}}
        ddl_col = {"name": "CdProduto", "type": "int", "nullable": False}
        result = fe._extract_filter_heuristic("propostas cdc", col, [ddl_col], FLOW_DATA)
        assert result is not None
        assert result.resolved_via == "alias_match"

    def test_heuristic_returns_status_filter(self):
        """Cobre linhas 132-134: _extract_filter_heuristic com coluna de status"""
        fe = FilterExtractor(MagicMock())
        col = {"name": "StatusProposta", "ai_hints": {"nl_terms_seed": []}}
        ddl_col = {"name": "StatusProposta", "type": "varchar(30)", "nullable": True}
        result = fe._extract_filter_heuristic("propostas aprovadas", col, [ddl_col], {})
        assert result is not None
        assert result.resolved_via == "heuristic_status"
