FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 DBT_PROFILES_DIR=/dbt
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
WORKDIR /dbt
# Le projet est monté en volume : ./dbt -> /dbt
CMD ["sh", "/dbt/run_loop.sh"]
