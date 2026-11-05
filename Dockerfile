FROM apache/airflow:2.10.5-python3.12

COPY requirements-runtime.txt /opt/airflow/requirements-runtime.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements-runtime.txt
