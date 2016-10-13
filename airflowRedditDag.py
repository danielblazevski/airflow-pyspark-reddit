from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import boto3
import json
from sets import Set

s3Key = '2007/RC_2007-10'
redditFile = 'RC-s3-2007-10'

def downloadRedditS3():
	s3 = boto3.resource('s3')
	s3.meta.client.download_file('reddit-comments', s3Key, redditFile)

def numUniqueAuthors():
	filename = redditFile
	authorSet = Set()
 
	with open(filename) as f:
		for i, line in enumerate(f):
			jsonLine = json.loads(line)
			if 'author' in jsonLine:
				authorSet.add(jsonLine['author'])
	print len(authorSet)

def averageUpvote():
	filename = redditFile
	count = 0
	total = 0
	with open(filename) as f:
		for i, line in enumerate(f):
			jsonLine = json.loads(line)
			if 'ups' in jsonLine:
				count += 1
				total += jsonLine['ups']
	print total/count

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    's3Reddit2', default_args=default_args, schedule_interval=timedelta(1))

download_data = PythonOperator(
    task_id='download_data',
    python_callable=downloadRedditS3,
    dag=dag)

process_data = PythonOperator(
    task_id='process_data',
    python_callable=numUniqueAuthors,
    dag=dag)

process_data.set_upstream(download_data)

