# etl/extract.py
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import logging
from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log
from config.settings import TICKERS, DIAS_HISTORICO_INICIAL

# ✅ Solo obtenemos el logger — no configuramos nada
logger = logging.getLogger(__name__)


def get_fecha_inicio(dias: int) -> str:
    fecha = datetime.today() - timedelta(days=dias)
    return fecha.strftime("%Y-%m-%d")


@retry(
    stop=stop_after_attempt(3),                          # máximo 3 intentos
    wait=wait_exponential(multiplier=1, min=2, max=10),  # espera 2s, 4s, 8s...
    before_sleep=before_sleep_log(logger, logging.WARNING),  # loguea cada reintento
    reraise=True  # si agota los intentos, relanza la excepción original
)
def extraer_datos_ticker(ticker: str, fecha_inicio: str) -> pd.DataFrame | None:
    """
    Descarga el historial de precios de un ticker desde Yahoo Finance.
    El decorador @retry maneja automáticamente los reintentos si falla.
    """
    try:
        logger.info(f"Extrayendo datos para: {ticker}")
        activo = yf.Ticker(ticker)
        df = activo.history(start=fecha_inicio)

        if df.empty:
            logger.warning(f"Sin datos para {ticker}. Puede ser un ticker incorrecto.")
            return None

        df["ticker"] = ticker
        df = df.reset_index()
        logger.info(f"{ticker}: {len(df)} registros extraídos ({fecha_inicio} → hoy)")
        return df

    except Exception as e:
        logger.error(f"Error extrayendo {ticker}: {e}")
        raise  # re-lanzamos para que @retry lo capture y reintente


def extraer_todos() -> dict[str, pd.DataFrame]:
    fecha_inicio = get_fecha_inicio(DIAS_HISTORICO_INICIAL)
    logger.info(f"Inicio de extracción. Fecha desde: {fecha_inicio}")

    resultados = {}
    for ticker in TICKERS.keys():
        try:
            df = extraer_datos_ticker(ticker, fecha_inicio)
            if df is not None:
                resultados[ticker] = df
        except Exception as e:
            # Si agota los 3 reintentos, logueamos pero continuamos con los demás tickers
            logger.error(f"[{ticker}] Falló después de 3 intentos: {e}")

    logger.info(f"Extracción completa. Tickers exitosos: {len(resultados)}/{len(TICKERS)}")
    return resultados


if __name__ == "__main__":
    datos = extraer_todos()
    for ticker, df in datos.items():
        print(f"\n{'='*40}")
        print(f"Ticker: {ticker} — {len(df)} filas")
        print(df[["Date", "Open", "Close", "High", "Low", "Volume"]].head(3))