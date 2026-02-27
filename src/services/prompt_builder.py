"""
Prompt Builder
==============
Prompts compartilhados entre os serviços de AI.
"""


def build_scan_prompt(user_query: str, flow_id: str, catalog: list) -> str:
    """
    Prompt unificado: seleciona tabela + extrai filtros em uma única chamada.
    """
    all_cols_lines = []
    for t in catalog:
        for c in t.get("columns", []):
            all_cols_lines.append(f"  - {c['full_ref']} ({c['type']})")

    fk_lines = []
    for t in catalog:
        schema = t.get("schema", "")
        tname  = t.get("table", "")
        for fk in t.get("foreign_keys", []):
            from_cols = fk.get("from_columns", [])
            to_table  = fk.get("to_table", "")
            to_cols   = fk.get("to_columns", [])
            if from_cols and to_cols:
                left  = f"{schema}.{tname}.{from_cols[0]}" if schema else f"{tname}.{from_cols[0]}"
                right = f"{schema}.{to_table}.{to_cols[0]}" if schema else f"{to_table}.{to_cols[0]}"
                fk_lines.append(f"  - {left} → {right}")

    tables_summary = "\n".join(
        f"  - {t['table']} ({t.get('schema', '')}): {t.get('description', '')}"
        for t in catalog
    )
    cols_list = "\n".join(all_cols_lines) or "  (nenhuma)"
    fk_list   = "\n".join(fk_lines) or "  (nenhuma)"

    return f"""Você é um assistente que analisa consultas em linguagem natural e extrai informações estruturadas de banco de dados.

USER QUERY: "{user_query}"

BANCO DE DADOS: {flow_id}

TABELAS DISPONÍVEIS:
{tables_summary}

TODAS AS COLUNAS DISPONÍVEIS (use APENAS estas como "column"):
{cols_list}

FOREIGN KEYS (relacionamentos entre tabelas):
{fk_list}

TAREFA: Faça as duas coisas abaixo em uma única resposta:
1. Identifique a TABELA PRINCIPAL que responde ao user query.
2. Extraia os FILTROS necessários.

REGRAS OBRIGATÓRIAS:
- "selected_table" deve ser o nome exato de uma das tabelas listadas acima.
- "column" nos filtros deve ser o "full_ref" exato da lista de colunas.
- "value" deve ser APENAS o valor literal — NUNCA subselect, NUNCA SQL.
- Se o filtro envolver tabela relacionada via FK, use a coluna dessa tabela diretamente.
- Operadores válidos: =, >, <, >=, <=, IN, LIKE, BETWEEN
- Para datas: DATEADD(day, -N, GETDATE())

Retorne APENAS JSON (sem markdown):
{{
  "selected_table": "nome exato da tabela principal",
  "reasoning": "por que essa tabela e esses filtros",
  "confidence": 0.0-1.0,
  "filters": [
    {{
      "column": "full_ref exato",
      "operator": "=",
      "value": "valor literal",
      "nl_term": "termo do usuário",
      "confidence": 0.0-1.0
    }}
  ],
  "select_columns": [],
  "order_by": [{{"column": "full_ref", "direction": "ASC|DESC"}}],
  "limit": 0,
  "confidence_score": 0.0-1.0
}}"""
