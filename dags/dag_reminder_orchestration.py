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
    "retry_delay": timedelta(minutes=2),
}

def identify_pending_reminders(**context):
    """Identifica citas que entran en la ventana de recordatorio (T-24h o T-48h)."""
    logger.info("Buscando citas pendientes de recordatorio...")
    reminders = [
        {
            "appointment_id": 301,
            "patient_name": "Carlos Gomez",
            "channel": "sms",
            "routing_key": "reminder.sms.high_risk",
            "risk": "ALTO",
            "appointment_time": "2026-11-06 09:00:00"
        },
        {
            "appointment_id": 302,
            "patient_name": "Laura Perez",
            "channel": "email",
            "routing_key": "reminder.email.standard",
            "risk": "BAJO",
            "appointment_time": "2026-11-06 11:30:00"
        }
    ]
    context["ti"].xcom_push(key="pending_reminders", value=reminders)

def publish_reminders_to_rabbitmq(**context):
    """Publica cada recordatorio al exchange de RabbitMQ."""
    from src.services.messaging.producer import publish_reminder_event

    ti = context["ti"]
    reminders = ti.xcom_pull(key="pending_reminders", task_ids="get_reminders")

    for item in reminders:
        payload = {
            "appointment_id": item["appointment_id"],
            "patient": item["patient_name"],
            "risk": item["risk"],
            "appointment_time": item["appointment_time"],
            "dispatched_at": datetime.utcnow().isoformat()
        }
        success = publish_reminder_event(item["routing_key"], payload)
        if success:
            logger.info(f"Recordatorio de cita {item['appointment_id']} enviado a RabbitMQ.")
        else:
            raise RuntimeError(f"Fallo al publicar recordatorio de cita {item['appointment_id']}.")

with DAG(
    dag_id="dag_reminder_orchestration",
    default_args=default_args,
    description="Orquestación de recordatorios de citas con publicación a RabbitMQ",
    schedule_interval="*/30 * * * *",  # Cada 30 minutos
    catchup=False,
    tags=["clinicared", "reminders", "rabbitmq"],
) as dag:

    task_get_reminders = PythonOperator(
        task_id="get_reminders",
        python_callable=identify_pending_reminders,
    )

    task_publish_reminders = PythonOperator(
        task_id="publish_to_rabbitmq",
        python_callable=publish_reminders_to_rabbitmq,
    )

    task_get_reminders >> task_publish_reminders