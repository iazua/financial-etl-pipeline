# tests/test_transform.py
import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from etl.transform import (
    limpiar_fechas,
    renombrar_columnas,
    calcular_variacion_pct,
    validar_datos,
    transformar_ticker
)


# ─────────────────────────────────────────────
# FIXTURES: datos de prueba reutilizables
# ─────────────────────────────────────────────

# Un fixture es una función que prepara datos para los tests.
# @pytest.fixture hace que pytest la ejecute automáticamente antes de cada test que la pida.

@pytest.fixture
def df_crudo():
    """Simula el output de yfinance para un ticker — datos con timezone."""
    return pd.DataFrame({
        "Date":   pd.to_datetime(["2025-01-01", "2025-01-02", "2025-01-03"]).tz_localize("America/New_York"),
        "Open":   [150.0, 152.0, 151.0],
        "Close":  [152.0, 151.0, 153.0],
        "High":   [153.0, 154.0, 155.0],
        "Low":    [149.0, 150.0, 150.0],
        "Volume": [1000000, 1100000, 900000],
        "ticker": ["AAPL", "AAPL", "AAPL"]
    })


@pytest.fixture
def df_transformado(df_crudo):
    """Aplica las transformaciones base para tener un df listo para validar."""
    df = limpiar_fechas(df_crudo.copy())
    df = renombrar_columnas(df)
    df = calcular_variacion_pct(df)
    return df


# ─────────────────────────────────────────────
# TESTS: limpiar_fechas
# ─────────────────────────────────────────────

def test_limpiar_fechas_elimina_timezone(df_crudo):
    """Las fechas no deben tener timezone después de la limpieza."""
    df = limpiar_fechas(df_crudo.copy())
    assert df["Date"].dt.tz is None, "La fecha aún tiene timezone"


def test_limpiar_fechas_conserva_cantidad_filas(df_crudo):
    """La limpieza no debe eliminar filas."""
    df = limpiar_fechas(df_crudo.copy())
    assert len(df) == len(df_crudo)


# ─────────────────────────────────────────────
# TESTS: renombrar_columnas
# ─────────────────────────────────────────────

def test_renombrar_columnas_nombres_correctos(df_crudo):
    """Las columnas deben estar en español después del renombre."""
    df = limpiar_fechas(df_crudo.copy())
    df = renombrar_columnas(df)
    columnas_esperadas = {"fecha", "precio_open", "precio_close", "precio_high", "precio_low", "volumen"}
    assert columnas_esperadas.issubset(set(df.columns)), \
        f"Faltan columnas. Presentes: {set(df.columns)}"


# ─────────────────────────────────────────────
# TESTS: calcular_variacion_pct
# ─────────────────────────────────────────────

def test_variacion_pct_primer_registro_es_nan(df_transformado):
    """El primer registro siempre debe ser NaN — no tiene día anterior."""
    assert pd.isna(df_transformado["variacion_pct"].iloc[0]), \
        "El primer valor debería ser NaN"


def test_variacion_pct_calculo_correcto(df_transformado):
    """
    Verifica el cálculo manualmente:
    Close día 1: 152.0, Close día 2: 151.0
    Variación: (151 - 152) / 152 * 100 = -0.6579
    """
    variacion_dia2 = df_transformado["variacion_pct"].iloc[1]
    esperado = round((151.0 - 152.0) / 152.0 * 100, 4)
    assert abs(variacion_dia2 - esperado) < 0.001, \
        f"Variación esperada {esperado}, obtenida {variacion_dia2}"


# ─────────────────────────────────────────────
# TESTS: validar_datos
# ─────────────────────────────────────────────

def test_validacion_pasa_con_datos_correctos(df_transformado):
    """Un DataFrame bien formado debe pasar todas las validaciones."""
    assert validar_datos(df_transformado, "AAPL") is True


def test_validacion_falla_con_precio_negativo(df_transformado):
    """Precios negativos deben hacer fallar la validación."""
    df = df_transformado.copy()
    df.loc[0, "precio_close"] = -1.0
    assert validar_datos(df, "AAPL") is False


def test_validacion_falla_high_menor_que_low(df_transformado):
    """Si high < low en algún registro, la validación debe fallar."""
    df = df_transformado.copy()
    df.loc[0, "precio_high"] = 100.0
    df.loc[0, "precio_low"]  = 200.0
    assert validar_datos(df, "AAPL") is False


def test_validacion_falla_con_fechas_duplicadas(df_transformado):
    """Fechas duplicadas deben hacer fallar la validación."""
    df = pd.concat([df_transformado, df_transformado.iloc[[0]]]).reset_index(drop=True)
    assert validar_datos(df, "AAPL") is False


def test_validacion_falla_con_df_vacio():
    """Un DataFrame vacío debe fallar la validación."""
    df_vacio = pd.DataFrame(columns=["fecha", "precio_open", "precio_close",
                                      "precio_high", "precio_low", "volumen"])
    assert validar_datos(df_vacio, "AAPL") is False


# ─────────────────────────────────────────────
# TESTS: pipeline completo
# ─────────────────────────────────────────────

def test_transformar_ticker_retorna_dataframe(df_crudo):
    """El pipeline completo debe retornar un DataFrame no vacío."""
    resultado = transformar_ticker(df_crudo.copy(), "AAPL")
    assert resultado is not None
    assert isinstance(resultado, pd.DataFrame)
    assert not resultado.empty


def test_transformar_ticker_columnas_finales(df_crudo):
    """El output final debe tener exactamente las columnas del modelo SQL."""
    resultado = transformar_ticker(df_crudo.copy(), "AAPL")
    columnas_esperadas = {
        "fecha", "ticker", "precio_open", "precio_close",
        "precio_high", "precio_low", "volumen", "variacion_pct"
    }
    assert set(resultado.columns) == columnas_esperadas