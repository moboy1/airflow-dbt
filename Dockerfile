FROM ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.7.3-python3.11}

USER root
RUN apt update && apt install -y git
#RUN apt update && apt install -y build-essential libffi-dev

USER airflow
#RUN pip install -r requirements.txt
RUN pip install apache-airflow-providers-airbyte[http] \
    && pip install apache-airflow-providers-airbyte \
    && pip install astronomer-cosmos \
    && pip install git+https://github.com/maver1ck/dbt-mysql
