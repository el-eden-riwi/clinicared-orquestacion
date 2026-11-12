import os
import json
import urllib.request
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import col, datediff, current_date, sum as _sum, count, when, max as _max

def extract_from_api():
    api_url = os.getenv("CLINICARED_API_URL", "").rstrip("/")
    with urllib.request.urlopen(f"{api_url}/patients/", timeout=15) as res:
        patients_json = json.loads(res.read().decode())
    with urllib.request.urlopen(f"{api_url}/appointments/", timeout=15) as res:
        appts_json = json.loads(res.read().decode())

    patients_rows = [
        (
            p["id"],
            f"{p['first_name']} {p['last_name']}".strip(),
            (date.today().year - date.fromisoformat(p["date_of_birth"]).year) if p.get("date_of_birth") else None
        )
        for p in patients_json
    ]
    appts_rows = [
        (a["id"], a["patient_id"], a["status"], date.fromisoformat(a["date"]))
        for a in appts_json
    ]
    print(f"[*] Extraídos {len(patients_rows)} pacientes y {len(appts_rows)} citas reales desde AWS Fargate/RDS.")
    return patients_rows, appts_rows

def run_etl():
    spark = SparkSession.builder \
        .appName("ClinicaRed_Risk_ETL_RDS") \
        .master("local[*]") \
        .getOrCreate()

    patients_schema = StructType([
        StructField("patient_id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ])
    appointments_schema = StructType([
        StructField("appointment_id", IntegerType(), False),
        StructField("patient_id", IntegerType(), True),
        StructField("status", StringType(), True),
        StructField("appointment_date", DateType(), True)
    ])

    patients_data, appointments_data = extract_from_api()

    df_patients = spark.createDataFrame(patients_data, schema=patients_schema).dropna(subset=["age"])
    df_appointments = spark.createDataFrame(appointments_data, schema=appointments_schema)

    df_joined = df_appointments.join(df_patients, on="patient_id", how="inner")
    df_transformed = df_joined.withColumn("is_no_show", when(col("status") == "NO_SHOW", 1).otherwise(0))

    df_aggregated = df_transformed.groupBy("patient_id", "name", "age").agg(
        count("appointment_id").alias("total_appointments"),
        _sum("is_no_show").alias("historical_no_shows"),
        _max("appointment_date").alias("last_visit_date")
    )

    df_final = df_aggregated.withColumn(
        "days_since_last_visit",
        datediff(current_date(), col("last_visit_date"))
    ).drop("last_visit_date")

    df_final.show()

    output_path = os.path.join("/opt/airflow/spark_jobs", "output_risk_data")
    df_final.coalesce(1).write.mode("overwrite").csv(output_path, header=True)
    print("[√] DataMart operacional generado con datos reales de AWS RDS.")
    spark.stop()

if __name__ == "__main__":
    run_etl()
