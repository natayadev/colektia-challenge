import os
import logging
import pandas as pd
from dotenv import load_dotenv

from etl.extract import extract_csv
from etl.transform import transform
from etl.load import load_to_parquet

"""
Exercise 3: ETL Pipeline with Pandas
This script implements an ETL pipeline.
It extracts data from a CSV file, transforms it,
and loads it into a Parquet file.
"""

# Cargar variables de entorno
load_dotenv()

# Configurar logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

RAW_DATA_PATH = os.getenv("RAW_DATA_PATH")
PROCESSED_DATA_PATH = os.getenv("PROCESSED_DATA_PATH")

if __name__ == "__main__":
    # Definir el orden de los pasos del ETL y sus validaciones
    logger.info("Inicio del proceso ETL")

    try:
        df = extract_csv(RAW_DATA_PATH)
        logger.info(f"Archivo extraído desde: {RAW_DATA_PATH}")
    except Exception as e:
        logger.error(f"Error en la extracción: {e}")
        raise

    try:
        df = transform(df)
        logger.info("Transformación aplicada correctamente")
    except Exception as e:
        logger.error(f"Error en la transformación: {e}")
        raise

    try:
        load_to_parquet(df, PROCESSED_DATA_PATH)
        logger.info(f"Archivo guardado en: {PROCESSED_DATA_PATH}")

        df_resultado = pd.read_parquet(PROCESSED_DATA_PATH)
        print("Vista previa del archivo .parquet:")
        print(df_resultado.head(10))
        print("Vista previa del esquema:")
        print(df_resultado.dtypes)
    
    except Exception as e:
        logger.error(f"Error en la carga: {e}")
        raise

    logger.info("Proceso ETL finalizado correctamente")
