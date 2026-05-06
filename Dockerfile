FROM apache/airflow:3.2.0

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk ant procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
RUN export JAVA_HOME

ENV PATH="$PATH:/home/airflow/.local/lib/python3.11/site-packages/pyspark/bin"

RUN mkdir -p /opt/airflow/spark_jars

RUN curl -sS https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -o /opt/airflow/spark_jars/hadoop-aws-3.3.4.jar && \
    curl -sS https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -o /opt/airflow/spark_jars/aws-java-sdk-bundle-1.12.262.jar && \
    curl -sS https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar -o /opt/airflow/spark_jars/postgresql-42.6.0.jar 

RUN chown -R airflow: /opt/airflow/spark_jars

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt