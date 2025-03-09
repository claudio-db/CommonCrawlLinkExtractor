FROM apache/spark-py:3.3.0

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        python3-dev \
        libpq-dev \
        wget \
        && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Download PostgreSQL JDBC driver (for Spark)
RUN wget https://jdbc.postgresql.org/download/postgresql-42.5.1.jar -O /opt/spark/jars/postgresql-42.5.1.jar

# Install dependencies and package
COPY requirements.txt /app/
WORKDIR /app

RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir psycopg2-binary

COPY . /app/

RUN pip install -e .

CMD ["bash"]