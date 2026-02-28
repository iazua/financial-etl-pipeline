# config/settings.py
import os
from dotenv import load_dotenv

# Carga las variables del archivo .env al entorno de Python
# En Azure, estas variables vendrán del servicio directamente — load_dotenv() no hace nada
load_dotenv()

TICKERS = {
    "AAPL":    {"nombre": "Apple Inc.",         "tipo": "accion", "sector": "Tecnología"},
    "MSFT":    {"nombre": "Microsoft Corp.",    "tipo": "accion", "sector": "Tecnología"},
    "BTC-USD": {"nombre": "Bitcoin",            "tipo": "cripto", "sector": None},
    "GC=F":    {"nombre": "Oro (Gold Futures)", "tipo": "futuro", "sector": None},
}

DIAS_HISTORICO_INICIAL = 90

# Leemos cada componente del entorno con un valor por defecto como fallback
_server = os.getenv("DB_SERVER", r".\SQLEXPRESS")
_db     = os.getenv("DB_NAME",   "financial_pipeline")
_driver = os.getenv("DB_DRIVER", "ODBC+Driver+17+for+SQL+Server")

DB_CONNECTION_STRING = (
    f"mssql+pyodbc://{_server}/{_db}"
    f"?driver={_driver}"
    f"&trusted_connection=yes"
)