"""
Script para configurar o nome do database no Firestore
"""
from dotenv import load_dotenv
from src.config.settings import Settings
from google.cloud import firestore
from google.oauth2 import service_account

load_dotenv()
settings = Settings.from_env()

# Conectar
if settings.firestore_credentials_path:
    creds = service_account.Credentials.from_service_account_file(
        settings.firestore_credentials_path
    )
    db = firestore.Client(
        project=settings.firestore_project_id,
        database=settings.firestore_database,
        credentials=creds,
    )
else:
    db = firestore.Client(
        project=settings.firestore_project_id,
        database=settings.firestore_database,
    )

# Configuração do database
COLLECTION = "adventureworks_lt"
DATABASE_CONFIG = {
    "database": {
        "name": "AdventureWorksLT2022",
        "type": "Sybase",
        "dialect": "tsql_sybase"
    }
}

print(f"\n{'='*70}")
print(f"Configurando database name para: {COLLECTION}")
print(f"{'='*70}\n")

# Adicionar configuração em um documento especial na coleção
config_doc_ref = db.collection(COLLECTION).document("_config")
config_doc_ref.set(DATABASE_CONFIG, merge=True)

print(f"✅ Configuração salva em: {COLLECTION}/_config")
print(f"   Database name: {DATABASE_CONFIG['database']['name']}")
print(f"   Database type: {DATABASE_CONFIG['database']['type']}")
print(f"\n{'='*70}\n")
