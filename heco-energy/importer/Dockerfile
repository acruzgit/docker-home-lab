FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir pandas influxdb-client
COPY import_csv_to_influx.py /app/
CMD ["python", "/app/import_csv_to_influx.py"]

