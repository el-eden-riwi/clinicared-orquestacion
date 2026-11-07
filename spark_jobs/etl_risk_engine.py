import os
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import col, datediff, current_date, sum as _sum, count, when, max as _max

def run_etl():
    # Inicializar Spark en modo local
    spark = SparkSession.builder \
        .appName("ClinicaRed_Risk_ETL") \
        .master("local[*]") \
        .getOrCreate()

    print("[*] Spark Session iniciada en el entorno de Orquestación.")

    # 1. ESQUEMAS EXPLÍCITOS (Requisito Task 4)
    patients_schema = StructType([
        StructField("patient_id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ])

    appointments_schema = StructType([
        StructField("appointment_id", StringType(), False),
        StructField("patient_id", StringType(), True),
        StructField("status", StringType(), True), 
        StructField("appointment_date", DateType(), True)
    ])

    # Datos crudos simulados
    patients_data = [
        ("P001", "Jeas Ayala", 25),
        ("P002", "Lyan Paez", 30),
        ("P003", "Kevin", None) # Dato nulo para probar el Cleansing
    ]

    appointments_data = [
        ("A1", "P001", "NO_SHOW", date(2026, 1, 15)),
        ("A2", "P001", "NO_SHOW", date(2026, 2, 20)),
        ("A3", "P001", "NO_SHOW", date(2026, 5, 10)),
        ("A4", "P001", "NO_SHOW", date(2026, 8, 5)),
        ("A5", "P002", "ATTENDED", date(2026, 10, 1)),
        ("A6", "P009", "ATTENDED", date(2026, 11, 1)) # Paciente huérfano para probar el Join
    ]

    df_patients = spark.createDataFrame(patients_data, schema=patients_schema)
    df_appointments = spark.createDataFrame(appointments_data, schema=appointments_schema)

    # 2. CLEANSING
    print("[*] Limpiando datos corruptos o nulos...")
    df_patients_clean = df_patients.dropna(subset=["age"])

    # 3. JOINS
    print("[*] Cruzando citas con pacientes válidos...")
    df_joined = df_appointments.join(df_patients_clean, on="patient_id", how="inner")

    # 4. TRANSFORMATIONS & AGGREGATIONS
    print("[*] Calculando métricas de riesgo...")
    df_transformed = df_joined.withColumn(
        "is_no_show", 
        when(col("status") == "NO_SHOW", 1).otherwise(0)
    )

    df_aggregated = df_transformed.groupBy("patient_id", "age").agg(
        count("appointment_id").alias("total_appointments"),
        _sum("is_no_show").alias("historical_no_shows"),
        _max("appointment_date").alias("last_visit_date")
    )

    df_final = df_aggregated.withColumn(
        "days_since_last_visit",
        datediff(current_date(), col("last_visit_date"))
    ).drop("last_visit_date")

    print("[*] Muestra de los datos procesados para el Motor de Riesgo:")
    df_final.show()

    # 5. OPERATIONAL OUTPUTS
    output_path = os.path.join(os.getcwd(), "spark_jobs", "output_risk_data")
    print(f"[*] Exportando DataMart operacional en: {output_path}")
    
    df_final.coalesce(1).write.mode("overwrite").csv(output_path, header=True)
    
    print("[√] Job de Spark completado exitosamente.")
    spark.stop()

if __name__ == "__main__":
    run_etl()