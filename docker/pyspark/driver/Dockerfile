FROM openjdk:8

WORKDIR /app

# Install Python
RUN apt-get update && \
    apt-get install -y python3.7 python3-pip && \
    rm -rf /var/lib/apt/lists/*

# Get postgres jdbc jar
# https://jdbc.postgresql.org/download.html#current
RUN wget -P spark_dependencies/ https://jdbc.postgresql.org/download/postgresql-42.2.16.jar

# Get spark
RUN wget https://archive.apache.org/dist/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz && \
    tar xvf spark-2.4.5-bin-hadoop2.7.tgz -C spark_dependencies/ && \
    rm -r spark-2.4.5-bin-hadoop2.7.tgz

COPY requirements.txt .

# I was experiencing failures here (Killed), so had to increase the memory allowed for Docker
RUN python3.7 -m pip install -r requirements.txt

COPY data data

COPY spark spark

ENV DATABASE_NAME="spark_stuff" POSTGRES_USER="pyspark" POSTGRS_DRIVER_JAR_PATH="spark_dependencies/postgresql-42.2.16.jar" POSTGRES_DRIVER_CLASS="org.postgresql.Driver"
# yes, shouldn't be here
ENV POSTGRES_PASSWORD "abc"

# Spark Web UI
EXPOSE 4040
# TODO get Web UI working

CMD python3.7 data/generate_data.py 3000 raw_data && \
    python3.7 spark/load_db.py raw_data
#     && \
#    python3.7 spark/aggregate.py
