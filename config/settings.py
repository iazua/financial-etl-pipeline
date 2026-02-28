# config/settings.py
import os
from dotenv import load_dotenv

load_dotenv()  # ← debe estar ANTES de cualquier os.getenv()

TICKERS = {
    "AAPL":    {"nombre": "Apple Inc.",         "tipo": "accion", "sector": "Tecnología"},
    "MSFT":    {"nombre": "Microsoft Corp.",    "tipo": "accion", "sector": "Tecnología"},
    "BTC-USD": {"nombre": "Bitcoin",            "tipo": "cripto", "sector": None},
    "GC=F":    {"nombre": "Oro (Gold Futures)", "tipo": "futuro", "sector": None},
}

DIAS_HISTORICO_INICIAL = 90

IS_AZURE = os.getenv("AZURE_FUNCTIONS_ENVIRONMENT") is not None

if IS_AZURE:
    from azure.keyvault.secrets import SecretClient
    from azure.identity import ManagedIdentityCredential
    credential = ManagedIdentityCredential()
    client = SecretClient(vault_url="https://kv-financial-etl.vault.azure.net/", credential=credential)
    DB_CONNECTION_STRING = client.get_secret("DB-CONNECTION-STRING").value
else:
    DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")