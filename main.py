# main.py
import logging
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler
from etl.extract import extraer_todos
from etl.transform import transformar_todos
from etl.load import cargar_todos


def configurar_logging():
    """
    Configura logging con dos destinos:
    - Consola: para desarrollo y monitoreo en tiempo real
    - Archivo rotativo: para historial persistente sin crecer infinitamente
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(module)s — %(message)s")

    # Handler de consola
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    # Handler de archivo rotativo
    # maxBytes=5MB, backupCount=3 → mantiene pipeline.log, pipeline.log.1, pipeline.log.2
    file_handler = RotatingFileHandler(
        "pipeline.log", maxBytes=5*1024*1024, backupCount=3, encoding="utf-8"
    )
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)


def run_pipeline():
    configurar_logging()
    logger = logging.getLogger(__name__)

    inicio = datetime.now()
    logger.info("=" * 50)
    logger.info("INICIANDO PIPELINE ETL — Financial Market Data")
    logger.info("=" * 50)

    try:
        logger.info("▶ FASE 1/3: Extracción")
        datos_crudos = extraer_todos()
        if not datos_crudos:
            raise ValueError("Extracción fallida — sin datos para procesar")

        logger.info("▶ FASE 2/3: Transformación")
        datos_limpios = transformar_todos(datos_crudos)
        if not datos_limpios:
            raise ValueError("Transformación fallida — sin datos limpios")

        logger.info("▶ FASE 3/3: Carga en SQL Server")
        cargar_todos(datos_limpios)

        duracion = (datetime.now() - inicio).seconds
        logger.info(f"✅ PIPELINE COMPLETADO en {duracion}s")

    except Exception as e:
        logger.error(f"❌ PIPELINE FALLIDO: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    run_pipeline()