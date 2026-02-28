# function_app.py
import azure.functions as func
import logging
import sys
import os

app = func.FunctionApp()

@app.timer_trigger(
    schedule="0 30 19 * * *",
    arg_name="myTimer",
    run_on_startup=False
)
def etl_pipeline(myTimer: func.TimerRequest) -> None:
    """
    Azure Function con Timer Trigger.
    Se ejecuta automáticamente según el schedule definido.
    """
    if myTimer.past_due:
        logging.warning("El timer está atrasado — la función no corrió en su horario programado")

    logging.info("▶ Azure Function disparada — iniciando pipeline ETL")

    try:
        # Importamos y ejecutamos el pipeline existente
        from etl.extract import extraer_todos
        from etl.transform import transformar_todos
        from etl.load import cargar_todos

        datos_crudos = extraer_todos()
        if not datos_crudos:
            raise ValueError("Extracción fallida — sin datos")

        datos_limpios = transformar_todos(datos_crudos)
        if not datos_limpios:
            raise ValueError("Transformación fallida — sin datos limpios")

        cargar_todos(datos_limpios)
        logging.info("✅ Pipeline ETL completado exitosamente desde Azure Function")

    except Exception as e:
        logging.error(f"❌ Pipeline fallido: {e}", exc_info=True)
        raise  # Re-lanzamos para que Azure registre la función como fallida