# etl/transform.py
import pandas as pd
import logging
from config.settings import TICKERS

logger = logging.getLogger(__name__)


def limpiar_fechas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza la columna Date:
    - Elimina la zona horaria (timezone-aware → timezone-naive)
    - Extrae solo la fecha sin hora
    """
    # .dt.tz_localize(None) elimina la info de zona horaria sin convertir la hora
    df["Date"] = pd.to_datetime(df["Date"]).dt.tz_localize(None).dt.normalize()
    return df


def renombrar_columnas(df: pd.DataFrame) -> pd.DataFrame:
    """Estandariza los nombres de columnas al español para que coincidan con SQL."""
    return df.rename(columns={
        "Date": "fecha",
        "Open": "precio_open",
        "Close": "precio_close",
        "High": "precio_high",
        "Low": "precio_low",
        "Volume": "volumen"
    })


def calcular_variacion_pct(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula el % de cambio del precio de cierre respecto al día anterior.

    Fórmula: ((precio_hoy - precio_ayer) / precio_ayer) * 100

    pandas tiene .pct_change() que hace exactamente esto.
    El primer registro siempre quedará como NaN porque no tiene día anterior.
    """
    df = df.sort_values("fecha").reset_index(drop=True)
    df["variacion_pct"] = df["precio_close"].pct_change() * 100
    df["variacion_pct"] = df["variacion_pct"].round(4)
    return df


def eliminar_columnas_innecesarias(df: pd.DataFrame) -> pd.DataFrame:
    """
    yfinance incluye columnas extra que no necesitamos en nuestro modelo.
    Conservamos solo las que definimos en fact_precios.
    """
    columnas_utiles = [
        "fecha", "ticker",
        "precio_open", "precio_close",
        "precio_high", "precio_low",
        "volumen", "variacion_pct"
    ]
    # Filtramos solo las columnas que existen en el DataFrame (defensive programming)
    columnas_presentes = [c for c in columnas_utiles if c in df.columns]
    return df[columnas_presentes]


def validar_datos(df: pd.DataFrame, ticker: str) -> bool:
    """
    Validaciones básicas de calidad de datos antes de cargar a SQL.
    Retorna True si el DataFrame pasa todas las validaciones.
    """
    errores = []

    # ¿Hay filas?
    if df.empty:
        errores.append("DataFrame vacío")

    # ¿Precios positivos?
    for col in ["precio_open", "precio_close", "precio_high", "precio_low"]:
        if (df[col] <= 0).any():
            errores.append(f"Valores <= 0 en {col}")

    # ¿High >= Low siempre? (regla financiera básica)
    if (df["precio_high"] < df["precio_low"]).any():
        errores.append("precio_high < precio_low en algún registro")

    # ¿Fechas duplicadas?
    if df["fecha"].duplicated().any():
        errores.append("Fechas duplicadas detectadas")

    if errores:
        for e in errores:
            logger.error(f"[{ticker}] Validación fallida: {e}")
        return False

    logger.info(f"[{ticker}] Validaciones OK — {len(df)} registros limpios")
    return True


def transformar_ticker(df: pd.DataFrame, ticker: str) -> pd.DataFrame | None:
    """
    Aplica el pipeline completo de transformación a un ticker.
    Cada paso es una función separada — esto se llama 'pipeline funcional'
    y facilita el testing y la depuración de cada etapa individualmente.
    """
    logger.info(f"Transformando: {ticker}")

    df = limpiar_fechas(df)
    df = renombrar_columnas(df)
    df = calcular_variacion_pct(df)
    df = eliminar_columnas_innecesarias(df)

    if not validar_datos(df, ticker):
        return None

    return df


def transformar_todos(datos_crudos: dict) -> dict[str, pd.DataFrame]:
    """
    Transforma todos los tickers extraídos.
    Recibe el output directo de extraer_todos().
    """
    datos_transformados = {}

    for ticker, df in datos_crudos.items():
        df_limpio = transformar_ticker(df.copy(), ticker)
        if df_limpio is not None:
            datos_transformados[ticker] = df_limpio

    logger.info(f"Transformación completa. Exitosos: {len(datos_transformados)}/{len(datos_crudos)}")
    return datos_transformados


if __name__ == "__main__":
    # Para probar este script aislado, primero extraemos
    from etl.extract import extraer_todos

    datos_crudos = extraer_todos()
    datos_limpios = transformar_todos(datos_crudos)

    for ticker, df in datos_limpios.items():
        print(f"\n{'=' * 40}")
        print(f"Ticker: {ticker} — {len(df)} filas")
        print(df.head(3).to_string())
        print(f"Tipos: {df.dtypes.to_dict()}")