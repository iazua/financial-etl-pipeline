# etl/load.py
import pandas as pd
from sqlalchemy import create_engine, text
import logging
from config.settings import TICKERS, DB_CONNECTION_STRING

logger = logging.getLogger(__name__)


def get_engine():
    """
    Crea la conexión a SQL Server usando SQLAlchemy.

    SQLAlchemy es un ORM (Object Relational Mapper) que nos permite
    interactuar con la base de datos usando Python en lugar de SQL puro.
    Aquí lo usamos principalmente por su integración con pandas (.to_sql).
    """
    try:
        engine = create_engine(DB_CONNECTION_STRING, fast_executemany=True)
        # Probamos la conexión
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Conexión a SQL Server establecida correctamente")
        return engine
    except Exception as e:
        logger.error(f"Error conectando a SQL Server: {e}")
        raise


# ─────────────────────────────────────────────
# 1. CARGA DE dim_activo
# ─────────────────────────────────────────────

def cargar_dim_activo(engine) -> dict[str, int]:
    """
    Inserta los activos en dim_activo si no existen todavía.
    Retorna un diccionario { 'AAPL': 1, 'MSFT': 2, ... } con los IDs de cada activo.
    Necesitamos estos IDs para insertar correctamente en fact_precios.
    """
    with engine.begin() as conn:  # begin() maneja el commit/rollback automáticamente
        for ticker, info in TICKERS.items():
            # IF NOT EXISTS: solo inserta si el ticker no está ya en la tabla
            conn.execute(text("""
                IF NOT EXISTS (SELECT 1 FROM dim_activo WHERE ticker = :ticker)
                    INSERT INTO dim_activo (ticker, nombre, tipo, sector)
                    VALUES (:ticker, :nombre, :tipo, :sector)
            """), {
                "ticker": ticker,
                "nombre": info["nombre"],
                "tipo": info["tipo"],
                "sector": info["sector"]
            })

    # Leer los IDs generados (o ya existentes) para usarlos después
    with engine.connect() as conn:
        resultado = conn.execute(text("SELECT ticker, activo_id FROM dim_activo"))
        activo_ids = {row.ticker: row.activo_id for row in resultado}

    logger.info(f"dim_activo lista. Activos registrados: {activo_ids}")
    return activo_ids


# ─────────────────────────────────────────────
# 2. CARGA DE dim_fecha
# ─────────────────────────────────────────────

def cargar_dim_fecha(engine, fechas: pd.Series) -> dict:
    """
    Inserta las fechas únicas en dim_fecha si no existen.
    Retorna un diccionario { fecha: fecha_id } para mapear en fact_precios.
    """
    fechas_unicas = pd.to_datetime(fechas.unique())

    with engine.begin() as conn:
        for fecha in fechas_unicas:
            conn.execute(text("""
                IF NOT EXISTS (SELECT 1 FROM dim_fecha WHERE fecha = :fecha)
                    INSERT INTO dim_fecha (fecha, anio, mes, dia, dia_semana)
                    VALUES (:fecha, :anio, :mes, :dia, :dia_semana)
            """), {
                "fecha": fecha.date(),
                "anio": fecha.year,
                "mes": fecha.month,
                "dia": fecha.day,
                "dia_semana": fecha.strftime("%A")  # Ej: "Monday"
            })

    with engine.connect() as conn:
        resultado = conn.execute(text("SELECT fecha, fecha_id FROM dim_fecha"))
        # Convertimos la fecha a string para usar como clave del diccionario
        fecha_ids = {str(row.fecha): row.fecha_id for row in resultado}

    logger.info(f"dim_fecha lista. Fechas registradas: {len(fecha_ids)}")
    return fecha_ids


# ─────────────────────────────────────────────
# 3. CARGA DE fact_precios
# ─────────────────────────────────────────────

def cargar_fact_precios(engine, df: pd.DataFrame, activo_ids: dict, fecha_ids: dict, ticker: str):
    """
    Inserta registros en fact_precios verificando duplicados antes de insertar.
    Estrategia: consultar IDs ya existentes → insertar solo los nuevos (bulk insert).
    """
    activo_id = activo_ids[ticker]

    with engine.connect() as conn:
        # Traemos todos los fecha_id ya existentes para este activo
        # Así evitamos consultar fila por fila — mucho más eficiente
        resultado = conn.execute(text("""
            SELECT fecha_id FROM fact_precios WHERE activo_id = :activo_id
        """), {"activo_id": activo_id})
        fecha_ids_existentes = {row.fecha_id for row in resultado}

    # Construimos solo las filas nuevas
    filas_nuevas = []
    for _, row in df.iterrows():
        fecha_key = str(row["fecha"].date())
        fecha_id  = fecha_ids.get(fecha_key)

        if fecha_id is None:
            logger.warning(f"fecha_id no encontrado para {fecha_key}, omitiendo.")
            continue

        if fecha_id in fecha_ids_existentes:
            continue  # Ya existe, saltamos

        variacion = None if pd.isna(row["variacion_pct"]) else float(row["variacion_pct"])

        filas_nuevas.append({
            "activo_id":    activo_id,
            "fecha_id":     fecha_id,
            "precio_open":  float(row["precio_open"]),
            "precio_close": float(row["precio_close"]),
            "precio_high":  float(row["precio_high"]),
            "precio_low":   float(row["precio_low"]),
            "volumen":      int(row["volumen"]),
            "variacion_pct": variacion
        })

    if not filas_nuevas:
        logger.info(f"[{ticker}] Sin registros nuevos — todos ya existían.")
        return

    # Bulk insert: insertar todas las filas nuevas en una sola transacción
    # Esto es mucho más rápido que insertar fila por fila
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO fact_precios (
                activo_id, fecha_id,
                precio_open, precio_close, precio_high, precio_low,
                volumen, variacion_pct
            ) VALUES (
                :activo_id, :fecha_id,
                :precio_open, :precio_close, :precio_high, :precio_low,
                :volumen, :variacion_pct
            )
        """), filas_nuevas)

    omitidos = len(df) - len(filas_nuevas)
    logger.info(f"[{ticker}] Insertados: {len(filas_nuevas)} | Ya existían: {omitidos}")

# ─────────────────────────────────────────────
# 4. ORQUESTADOR PRINCIPAL
# ─────────────────────────────────────────────

def cargar_todos(datos_transformados: dict):
    """
    Orquesta la carga completa: dimensiones primero, hechos después.
    """
    engine = get_engine()

    # Paso 1: cargar dimensión de activos
    activo_ids = cargar_dim_activo(engine)

    # Paso 2: recolectar todas las fechas únicas de todos los tickers
    todas_las_fechas = pd.concat([df["fecha"] for df in datos_transformados.values()])
    fecha_ids = cargar_dim_fecha(engine, todas_las_fechas)

    # Paso 3: cargar hechos por cada ticker
    for ticker, df in datos_transformados.items():
        cargar_fact_precios(engine, df, activo_ids, fecha_ids, ticker)

    logger.info("✅ Carga completa en SQL Server")


if __name__ == "__main__":
    from etl.extract import extraer_todos
    from etl.transform import transformar_todos

    datos_crudos = extraer_todos()
    datos_limpios = transformar_todos(datos_crudos)
    cargar_todos(datos_limpios)