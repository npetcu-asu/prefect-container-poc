FROM prefecthq/prefect:1.4.1-python3.10

COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install -r requirements.txt --no-cache-dir
COPY flows /opt/prefect/flows
CMD ["python", "/opt/prefect/flows/flow.py"]
