"""
Query Output Models
===================
Formato de saída para o agente de query (com PKs, FKs e schema completo).
"""

from typing import List, Dict, Any
from .intent import IntentObject


def convert_intent_to_query_format(intent: IntentObject,
                                   repository=None) -> Dict:
    """
    Converte IntentObject para o contrato esperado pelo agente de query.

    Formato de saída:
    {
      "parameters": {
        "filter_fields": [{"database.schema.table.col": "op value"}, ...],
        "tables": ["database.schema.table", ...],
        "return_fields":  ["database.schema.table.col", ...]
      },
      "ddl": [{
        "database": "...",
        "tipo": "SYBASE",
        "tables": [{ schema, name, columns, primaryKey?, foreignKeys? }, ...]
      }]
    }

    Args:
        intent:     IntentObject produzido pelo agente
        repository: FirestoreFirebaseRepository (opcional).
                    Se fornecido, busca tabelas relacionadas via
                    relationships_hints para compor o DDL completo.
    """
    database = intent.metadata.get('database_name', 'default')
    schema = intent.ddl_reference.schema
    table  = intent.table_name

    # Set para rastrear todas as tabelas usadas
    tables_used = set()
    tables_used.add(f"{database}.{schema}.{table}")

    # ── filter_fields ────────────────────────────────────────────
    filter_fields = []
    for f in intent.filters:
        val = f.value
        # Adicionar aspas se for string não numérica
        if isinstance(val, str) and not val.replace('.','').replace('-','').isnumeric() and not val.upper().startswith('DATEADD'):
            formatted_val = f"'{val}'"
        else:
            formatted_val = str(val)
        
        # Verificar se a coluna já vem com schema.table.column ou apenas column
        col = f.column
        if '.' in col:
            # Já vem com schema.table.column, apenas adicionar database
            full_column = f"{database}.{col}"
            # Extrair tabela do filtro (schema.table.column -> table)
            parts = col.split('.')
            if len(parts) >= 2:
                filter_table = f"{database}.{parts[0]}.{parts[1]}"
                tables_used.add(filter_table)
        else:
            # Apenas nome da coluna, adicionar database.schema.table
            full_column = f"{database}.{schema}.{table}.{col}"
        
        filter_fields.append({
            full_column: f"{f.operator} {formatted_val}"
        })

    # ── return_fields ────────────────────────────────────────────
    blocked = intent.metadata.get('blocked_columns', [])
    if intent.select_columns:
        return_fields = []
        for col in intent.select_columns:
            # Verificar se a coluna já vem com schema.table.column
            if '.' in col:
                full_col = f"{database}.{col}"
                # Extrair tabela
                parts = col.split('.')
                if len(parts) >= 2:
                    ret_table = f"{database}.{parts[0]}.{parts[1]}"
                    tables_used.add(ret_table)
            else:
                full_col = f"{database}.{schema}.{table}.{col}"
            return_fields.append(full_col)
    else:
        return_fields = [
            f"{database}.{schema}.{table}.{col['name']}"
            for col in intent.ddl_reference.columns_available
            if col['name'] not in blocked
        ]

    # ── DDL: tabela principal ─────────────────────────────────────
    main_table = _build_table_entry(
        schema=schema,
        name=table,
        columns=intent.ddl_reference.columns_available,
        constraints=intent.ddl_reference.constraints,
    )
    tables = [main_table]

    # ── DDL: tabelas relacionadas (via Firestore) ─────────────────
    if repository is not None:
        related = _fetch_related_tables(intent, repository)
        tables.extend(related)
        # Adiciona tabelas relacionadas à lista
        for rel_table in related:
            rel_schema = rel_table.get('schema', schema)
            rel_name = rel_table.get('name')
            tables_used.add(f"{database}.{rel_schema}.{rel_name}")

    ddl_output = {
        "database": database,
        "tipo":     intent.metadata.get('database_type', 'SYBASE'),
        "tables":   tables,
    }

    return {
        "parameters": {
            "filter_fields": filter_fields,
            "tables": sorted(list(tables_used)),  # Lista ordenada de tabelas únicas
            "return_fields": return_fields,
        },
        "ddl": [ddl_output],
    }


# ── helpers ──────────────────────────────────────────────────────

def _build_table_entry(schema: str, name: str,
                       columns: List[Dict], constraints: Dict) -> Dict:
    """Monta um item de tabela no formato do contrato."""
    entry: Dict[str, Any] = {
        "schema":  schema,
        "name":    name,
        "columns": [
            {
                "name":     col["name"],
                "type":     col.get("type", "string"),
                "nullable": col.get("nullable", True),
            }
            for col in columns
        ],
    }

    pk = constraints.get("primary_key", [])
    if pk:
        entry["primaryKey"] = pk

    fks = constraints.get("foreign_keys", [])
    if fks:
        entry["foreignKeys"] = [
            {
                "name":   fk.get("name", f"FK_{fk['column']}"),
                "column": fk["column"],
                "references": {
                    "table":  fk["references"]["table"],
                    "column": fk["references"]["column"],
                },
            }
            for fk in fks
        ]

    return entry


def _fetch_related_tables(intent: IntentObject, repository) -> List[Dict]:
    """
    Busca tabelas relacionadas usando relationships_hints do Firestore.

    Estratégia de resolução do DDL de cada tabela relacionada:
      1. Busca no próprio flow (get_ddl com o mesmo flow_id)
      2. Se não encontrar, varre todos os outros flows procurando
         um documento cuja table_definition.table_name bata
      3. Se ainda não encontrar, monta entrada mínima com as
         colunas de join conhecidas pelo hints
    """
    related: List[Dict] = []
    seen = {intent.table_name}

    try:
        # Buscar todas as tabelas do flow para encontrar relationships_hints
        all_tables = repository.get_tables_by_flow(intent.flow_id)
        
        # Procurar pela tabela principal para obter os hints
        main_table_data = None
        for table in all_tables:
            table_name = table.get('table_profile', {}).get('table_name', '')
            if table_name.lower() == intent.table_name.lower():
                main_table_data = table
                break
        
        if not main_table_data:
            return related

        hints = (
            main_table_data.get("relationships", {})
                .get("outgoing", [])
        )

        if not hints:
            return related

        for hint in hints:
            to_table = hint.get("to_table")
            if not to_table or to_table in seen:
                continue
            seen.add(to_table)

            # 1. Tentar no próprio flow
            related_ddl = repository.get_ddl(intent.flow_id, to_table)

            # 2. Buscar em outros flows (se necessário)
            if not related_ddl:
                # Tentar buscar em outros flows
                all_flows = repository.get_all_flows()
                for flow_id in all_flows.keys():
                    if flow_id == intent.flow_id:
                        continue
                    related_ddl = repository.get_ddl(flow_id, to_table)
                    if related_ddl:
                        print(f"   📎 DDL de {to_table} encontrado no flow: {flow_id}")
                        break

            # 3. Montar entrada
            if related_ddl:
                entry = _build_table_entry(
                    schema=related_ddl.get("schema", intent.ddl_reference.schema),
                    name=to_table,
                    columns=related_ddl.get("columns", []),
                    constraints=related_ddl.get("constraints", {}),
                )
            else:
                # Entrada mínima com colunas de join conhecidas
                join_cols = [
                    j.get("right", "").split(".")[-1]
                    for j in hint.get("join", [])
                    if j.get("right")
                ]
                print(f"   ⚠️  DDL de {to_table} não encontrado em nenhum flow — usando colunas de join")
                entry = {
                    "schema":  intent.ddl_reference.schema,
                    "name":    to_table,
                    "columns": [
                        {"name": col, "type": "string", "nullable": True}
                        for col in join_cols
                    ],
                }

            related.append(entry)

    except Exception as e:
        print(f"⚠️  Não foi possível buscar tabelas relacionadas: {e}")

    return related
