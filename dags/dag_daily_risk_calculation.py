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
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

def fetch_upcoming_7_days_appointments(**context):
    """Consulta citas médicas de los próximos 7 días."""
    logger.info("Consultando citas programadas para la ventana de 7 días...")
    # Citas simuladas para evaluación
    appointments = [
        {"appointment_id": 201, "patient_id": 501, "previous_no_shows": 3, "scheduled_days_ahead": 15},
        {"appointment_id": 202, "patient_id": 502, "previous_no_shows": 0, "scheduled_days_ahead": 2},
        {"appointment_id": 203, "patient_id": 503, "previous_no_shows": 1, "scheduled_days_ahead": 8},
    ]
    context["ti"].xcom_push(key="upcoming_appointments", value=appointments)

def evaluate_risk_rules(**context):
    """Aplica la matriz de reglas para clasificar el riesgo de inasistencia."""
    ti = context["ti"]
    appointments = ti.xcom_pull(key="upcoming_appointments", task_ids="fetch_appointments")
    evaluated = []

    for appt in appointments:
        no_shows = appt["previous_no_shows"]
        lead_time = appt["scheduled_days_ahead"]

        # Regla de clasificación de riesgo
        if no_shows >= 2 or (no_shows >= 1 and lead_time > 10):
            risk_level = "ALTO"
        elif no_shows == 1 or lead_time > 7:
            risk_level = "MEDIO"
        else:
            risk_level = "BAJO"

        evaluated.append({
            "appointment_id": appt["appointment_id"],
            "patient_id": appt["patient_id"],
            "risk_classification": risk_level,
            "calculated_at": datetime.utcnow().isoformat()
        })
        logger.info(f"Cita {appt['appointment_id']} evaluada con riesgo: {risk_level}")

    ti.xcom_push(key="evaluated_risk", value=evaluated)

def update_risk_in_db(**context):
    """Actualiza la clasificación en PostgreSQL para la toma de decisiones."""
    ti = context["ti"]
    results = ti.xcom_pull(key="evaluated_risk", task_ids="compute_risk")
    logger.info(f"Actualizando {len(results)} registros con su nuevo nivel de riesgo en BD.")

with DAG(
    dag_id="dag_daily_risk_calculation",
    default_args=default_args,
    description="Cálculo diario de riesgo de inasistencia para citas a 7 días",
    schedule_interval="0 0 * * *",  # Diario a medianoche
    catchup=False,
    tags=["clinicared", "risk_engine", "no_show"],
) as dag:

    task_fetch = PythonOperator(
        task_id="fetch_appointments",
        python_callable=fetch_upcoming_7_days_appointments,
    )

    task_compute = PythonOperator(
        task_id="compute_risk",
        python_callable=evaluate_risk_rules,
    )

    task_save = PythonOperator(
        task_id="update_risk_db",
        python_callable=update_risk_in_db,
    )

    task_fetch >> task_compute >> task_save