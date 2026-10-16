FROM apache/airflow:2.9.3

USER airflow
COPY requirements.txt .
RUN cat requirements.txt
RUN pip install --upgrade pip
RUN for i in 1 2 3 4 5 6; do pip install --timeout 120 --retries 10 -r requirements.txt && s=0 && break || s=$?; echo "pip attempt $i failed (rc=$s), retrying..."; sleep 15; done; exit $s
