from datetime import timedelta
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2),
    #'end_date' : datetime(2018, 12, 30),
    'depends_on_past': False,
    'email': ['arunprasad_bh@yahoo.com'],
    'email_on_failure': False,
    'email_on_success': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'tutorial1',
    default_args = default_args,
    description='My First DAG',
    schedule_interval=timedelta(days=1)
)

t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)

t1.doc_md="""\
#### Task Documentation
You can document your task using the attributes 'doc_md'(markdown),
'doc' (plain_text), 'doc_rst', 'doc_json', 'doc_yaml' which gets
rendered in the UI's Task Instance Details page.
![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
"""

dag.doc_md = __doc__

t2 = BashOperator(
    task_id='sleep',
    bash_command='sleep 5',
    depends_on_past=False,
    dag=dag,
)

templated_command = """
{% for i in range(5) %}
    echo "{{ ds }}"
    echo "{{ macros.ds_add(ds, 7) }}"
    echo "{{ params.my_param }}"
{% endfor %}
"""
t3 = BashOperator(
    task_id='templated',
    depends_on_past=False,
    bash_command=templated_command,
    params={'my_param': 'Parameter I passed in'},
    dag=dag,
)

create_file_command = """
/Users/arunabhamidipati/ArunProjects/BashScripts/create_file.sh
"""
t4 = BashOperator(
    task_id="shell_script1",
    depends_on_past=False,
    bash_command="scripts/create_file.sh ",
    dag=dag,
)

echo_command = """
echo "{{ params.my_param }}"
"""
t5 = BashOperator(
    task_id='print_success',
    bash_command=echo_command,
    params={'my_param': 'Success'},
    dag=dag,
)

t1.set_downstream(t2)
t3.set_upstream(t1)
t4.set_upstream(t3)
t5.set_upstream(t4)
"""
Another way of setting this is t1.set_downstream([t2,t3])
"""