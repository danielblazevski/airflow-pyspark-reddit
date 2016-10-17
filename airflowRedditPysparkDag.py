from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os

s3Bucket = 'reddit-comments'
s3Key = '2007/RC_2007-10'
redditFile = os.getcwd() + '/data/RC-s3-2007-10'
srcDir = os.getcwd() + '/src/'

sparkSubmit = '/usr/local/spark/bin/spark-submit'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
#    'start_date': datetime(2016, 10, 14, 16, 49),
	'start_date': datetime.now() - timedelta(seconds=45),
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('s3RedditPyspark', default_args=default_args, schedule_interval=timedelta(seconds=45))

downloadData= BashOperator(
    task_id='download-data',
    bash_command='python ' + srcDir + 'python/s3-reddit.py ' + s3Bucket + ' ' + s3Key + ' ' + redditFile,
    dag=dag)

numUniqueAuthors = BashOperator(
    task_id='Unique-authors',
    bash_command=sparkSubmit + ' ' + srcDir + 'pyspark/numUniqueAuthors.py ' + redditFile,
    dag=dag)
numUniqueAuthors.set_upstream(downloadData)

averageUpvotes = BashOperator(
	task_id='average-upvotes',
	bash_command=sparkSubmit + ' ' + srcDir + 'pyspark/averageUpvote.py ' + redditFile,
	dag=dag)
averageUpvotes.set_upstream(downloadData)
