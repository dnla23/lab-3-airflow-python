"""
DAG для анализа топ-5 самых прослушиваемых артистов в жанре Rock
Вариант задания №25
Автор: Студент
Дата: 2025
"""
from datetime import datetime, timedelta
import pandas as pd
import json
import sqlite3
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['test@example.com']
}

dag = DAG(
    'music_top_rock_artists_analysis',
    default_args=default_args,
    description='Анализ топ-5 самых прослушиваемых артистов в жанре Rock',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['etl', 'music', 'rock', 'variant_25']
)

DATA_DIR = '/opt/airflow/dags/data'
DB_PATH = '/opt/airflow/music_rock_analysis.db'

def extract_tracks(**context):
    print("Извлечение данных о треках из tracks.csv...")
    df = pd.read_csv(os.path.join(DATA_DIR, 'tracks.csv'))
    context['task_instance'].xcom_push(key='tracks_data', value=df.to_dict('records'))

def extract_listens(**context):
    print("Извлечение данных о прослушиваниях из listens.xlsx...")
    df = pd.read_excel(os.path.join(DATA_DIR, 'listens.xlsx'))
    context['task_instance'].xcom_push(key='listens_data', value=df.to_dict('records'))

def extract_artists(**context):
    print("Извлечение данных об артистах из artists.json...")
    with open(os.path.join(DATA_DIR, 'artists.json'), 'r', encoding='utf-8') as f:
        data = json.load(f)
    context['task_instance'].xcom_push(key='artists_data', value=data)

def transform_data(**context):
    print("Трансформация: поиск топ-5 артистов в жанре Rock...")
    tracks = pd.DataFrame(context['task_instance'].xcom_pull(key='tracks_data', task_ids='extract_tracks'))
    listens = pd.DataFrame(context['task_instance'].xcom_pull(key='listens_data', task_ids='extract_listens'))
    artists = pd.DataFrame(context['task_instance'].xcom_pull(key='artists_data', task_ids='extract_artists'))

    # Фильтрация треков по жанру Rock
    rock_tracks = tracks[tracks['genre'] == 'Rock']

    # Джойны
    merged = (listens
              .merge(rock_tracks, on='track_id')
              .merge(artists, on='artist_id'))

    # Агрегация: считаем прослушивания на артиста
    top_artists = (merged
                   .groupby(['artist_id', 'artist_name'])
                   .size()
                   .reset_index(name='listen_count')
                   .sort_values('listen_count', ascending=False)
                   .head(5))

    context['task_instance'].xcom_push(key='top_rock_artists', value=top_artists.to_dict('records'))

def load_to_database(**context):
    print("Загрузка топ-5 артистов в SQLite...")
    data = context['task_instance'].xcom_pull(key='top_rock_artists', task_ids='transform_data')
    df = pd.DataFrame(data)
    conn = sqlite3.connect(DB_PATH)
    df.to_sql('top_rock_artists', conn, if_exists='replace', index=False)
    conn.close()

def generate_report(**context):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM top_rock_artists", conn)
    conn.close()

    report = f"""ТОП-5 САМЫХ ПРОСЛУШИВАЕМЫХ АРТИСТОВ В ЖАНРЕ ROCK
==================================================
Дата анализа: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Общее количество артистов в выборке: {len(df)}

Рейтинг:
"""
    for i, row in df.iterrows():
        report += f"{i+1}. {row['artist_name']} — {row['listen_count']} прослушиваний\n"

    report_path = '/opt/airflow/rock_artists_report.txt'
    csv_path = '/opt/airflow/rock_artists_data.csv'
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(report)
    df.to_csv(csv_path, index=False)

    context['task_instance'].xcom_push(key='report', value=report)
    context['task_instance'].xcom_push(key='report_file_path', value=report_path)
    context['task_instance'].xcom_push(key='csv_file_path', value=csv_path)

def send_email_with_attachments(**context):
    from airflow.utils.email import send_email
    import os

    report = context['task_instance'].xcom_pull(key='report', task_ids='generate_report')
    files = []
    for f in ['/opt/airflow/rock_artists_report.txt', '/opt/airflow/rock_artists_data.csv']:
        if os.path.exists(f):
            files.append(f)

    html = f"<h3>ТОП-5 артистов Rock</h3><pre>{report}</pre>"
    send_email(
        to=['test@example.com'],
        subject='Топ-5 артистов в жанре Rock — Результаты анализа',
        html_content=html,
        files=files
    )

# Задачи
extract_tracks_task = PythonOperator(task_id='extract_tracks', python_callable=extract_tracks, dag=dag)
extract_listens_task = PythonOperator(task_id='extract_listens', python_callable=extract_listens, dag=dag)
extract_artists_task = PythonOperator(task_id='extract_artists', python_callable=extract_artists, dag=dag)

transform_task = PythonOperator(task_id='transform_data', python_callable=transform_data, dag=dag)
load_task = PythonOperator(task_id='load_to_database', python_callable=load_to_database, dag=dag)
report_task = PythonOperator(task_id='generate_report', python_callable=generate_report, dag=dag)
email_task = PythonOperator(task_id='send_email_notification', python_callable=send_email_with_attachments, dag=dag)

# Зависимости
[extract_tracks_task, extract_listens_task, extract_artists_task] >> transform_task
transform_task >> load_task >> report_task >> email_task