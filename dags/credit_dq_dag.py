"""DAG Airflow : génération -> contrôles qualité PySpark -> publication du rapport.

Le DAG est volontairement mince : la logique est dans `src/dq` (testée avec pytest).
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = "{{ var.value.get('dq_project_dir', '/opt/airflow/project') }}"

# Les données synthétiques contiennent ~15 % d'anomalies par construction : seuil de démo à 20 %.
# En usage réel, fixer le seuil avec les métiers (le job échoue au-delà).
MAX_ANOMALY_PCT = 20

with DAG(
    dag_id="credit_data_quality",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 1},
    tags=["data-quality", "spark"],
) as dag:
    generate = BashOperator(
        task_id="generate_synthetic_loans",
        bash_command=f"cd {PROJECT} && PYTHONPATH=src python -m dq.generate --rows 100000 --out data/loans.parquet",
    )
    check = BashOperator(
        task_id="run_quality_checks",
        bash_command=f"cd {PROJECT} && PYTHONPATH=src python -m dq.run --input data/loans.parquet --max-anomaly-pct {MAX_ANOMALY_PCT}",
    )
    generate >> check
