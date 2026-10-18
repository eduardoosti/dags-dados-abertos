######################################
# Criado por: Equipe IA / CGE         #
# Data criação: 28/07/2026           #
# Ingestão RFB CNPJ para Bronze       #
######################################

from datetime import datetime
from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator
from pendulum import duration

default_args = {
    "owner": "data-team",
    "retries": 3,
    "retry_delay": duration(seconds=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": duration(hours=2),
}

with DAG(
    dag_id="ingestao_receitafed_cnpj",
    default_args=default_args,
    max_active_runs=1,
    max_active_tasks=8,
    catchup=False,
    start_date=datetime(2025, 12, 1),
    schedule="0 5 L * *",
    tags=["Ingestão", "Receita Federal", "CNPJ", "Dados Abertos", "Bronze"],
) as dag:

    inicio = EmptyOperator(task_id="inicio")
    fim = EmptyOperator(task_id="fim")

    processar_rfb_cnpj = BashOperator(
        task_id="processar_ingestao_rfb_cnpj",
        bash_command=r"""
set -e
exec spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  --driver-memory 1g --executor-memory 1g \
  --executor-cores 2 --total-executor-cores 4 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.104.1 \
  --conf spark.driver.host=airflow-worker \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.driver.port=39000 --conf spark.blockManager.port=39001 \
  --conf spark.jars.ivy=/tmp/rfb-ivy \
  /opt/airflow/dags/receita_federal/processar_b_receitafed_cnpj.py
""",
        env={"RFB_REFERENCE_MONTH": "{{ dag_run.conf.get('reference_month') or data_interval_end.strftime('%Y-%m') }}"},
        append_env=True,
        skip_on_exit_code=None,
    )

    inicio >> processar_rfb_cnpj >> fim