from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


default_args = {
	"owner": "Airflow",
	"start_date": datetime(2019, 10, 30),
	"depends_on_past": False,
	"email_on_failure": False,
	"email_on_retry": False,
	"email": "sulman@localworker.xyz",
	"retries": 1
}

with DAG("macro_and_template", schedule_interval=None, default_args=default_args) as dag:
	task_1 = BashOperator(
			task_id="display",
			bash_command="echo EXECUTION DATA IS {{ execution_date }}",
			provide_context=True,
		)

	task_1