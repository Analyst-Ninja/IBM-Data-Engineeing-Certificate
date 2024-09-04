from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta


default_args = {
    'owner' : 'rohit',
    'start_date' : days_ago(0),
    'email' : 'youremail@abc.com'
}

dag = DAG(
    dag_id = 'process_web_log',
    schedule_interval = timedelta(days=1),
    default_args = default_args,
)

extract_data = BashOperator(
    task_id = 'extract',
    dag = dag,
    bash_command="cut -d ' ' -f1 /home/de-ninja/Documents/Courses/IBM_DE_Certificate/Capstone_project/module5/part2/accesslog.txt > /home/de-ninja/Documents/Courses/IBM_DE_Certificate/Capstone_project/module5/part2/extracted_data.txt" 
)

transform_data = BashOperator(
    task_id = 'transform_data',
    dag = dag,
    bash_command = "cat /home/de-ninja/Documents/Courses/IBM_DE_Certificate/Capstone_project/module5/part2/extracted_data.txt | grep '198.46.149.143' > /home/de-ninja/Documents/Courses/IBM_DE_Certificate/Capstone_project/module5/part2/transformed_data.txt" 
)

load_data = BashOperator(
    task_id = 'load_data',
    dag = dag,
    bash_command = "tar -cvf weblog.tar /home/de-ninja/Documents/Courses/IBM_DE_Certificate/Capstone_project/module5/part2/transformed_data.txt" 
)

extract_data >> transform_data >> load_data