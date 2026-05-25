FROM apache/spark:3.5.0-python3

USER root

WORKDIR /app

# Install test dependencies only — PySpark is already provided by the base image
COPY requirements-test.txt .
RUN pip install --no-cache-dir -r requirements-test.txt

# Copy source and tests into the image
COPY glue_jobs/ ./glue_jobs/
COPY tests/     ./tests/
COPY stubs/     ./stubs/
COPY pytest.ini .

# stubs/ provides awsglue shims for both driver and Spark worker processes
ENV PYTHONPATH="/app:/app/stubs:${PYTHONPATH}"

# Suppress verbose Spark/Hadoop output during test runs
ENV SPARK_LOCAL_IP="127.0.0.1"
ENV PYSPARK_PYTHON="python3"

CMD ["python3", "-m", "pytest", "tests/", "-v", "--tb=short"]
