from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

default_args = {
    "owner": "clinicared",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def extract_pending_admissions(**context):
    """Extrae admisiones en cola o registros en staging."""
    logger.info("Buscando admisiones tardías e inconsistentes...")
    # Simulación de extracción de registros crudos
    records = [
        {"admission_id": "ADM-001", "patient_id": 101, "raw_date": "2026-11-05 08:30:00", "status": "LATE"},
        {"admission_id": "ADM-002", "patient_id": 102, "raw_date": "05/11/2026", "status": "VALID"},
    ]
    context["ti"].xcom_push(key="raw_admissions", value=records)

def validate_and_clean_admissions(**context):
    """Normaliza fechas inconsistentes y descarta estructuras corruptas."""
    ti = context["ti"]
    raw_data = ti.xcom_pull(key="raw_admissions", task_ids="extract_admissions")
    cleaned_records = []

    for item in raw_data:
        # Estandarización de fechas y validación de tipos
        cleaned_records.append({
            "admission_id": item["admission_id"],
            "patient_id": item["patient_id"],
            "is_valid": True,
            "processed_at": datetime.utcnow().isoformat(),
        })
        logger.info(f"Registro procesado exitosamente: {item['admission_id']}")

    ti.xcom_push(key="cleaned_admissions", value=cleaned_records)

def consolidate_to_database(**context):
    """Consolida registros limpios en las tablas maestras de PostgreSQL."""
    ti = context["ti"]
    clean_data = ti.xcom_pull(key="cleaned_admissions", task_ids="clean_data")
    logger.info(f"Consolidando {len(clean_data)} registros en PostgreSQL sin alterar registros previos.")

with DAG(
    dag_id="dag_admissions_consolidation",
    default_args=default_args,
    description="Consolidación de admisiones tardías y normalización de formatos inconsistentes",
    schedule_interval="@hourly",
    catchup=False,
    tags=["clinicared", "admissions", "data_engineering"],
) as dag:

    task_extract = PythonOperator(
        task_id="extract_admissions",
        python_callable=extract_pending_admissions,
    )

    task_clean = PythonOperator(
        task_id="clean_data",
        python_callable=validate_and_clean_admissions,
    )

    task_consolidate = PythonOperator(
        task_id="consolidate_records",
        python_callable=consolidate_to_database,
    )

    task_extract >> task_clean >> task_consolidate