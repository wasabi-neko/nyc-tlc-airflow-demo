# image for x-airflow-common
# It loads requirements from `requirements.txt` and pip install them
FROM apache/airflow:2.10.2
ADD requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt