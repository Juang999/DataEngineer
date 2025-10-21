FROM apache/airflow:3.0.0-python3.10

# Set environment
ENV HOST=0.0.0.0

# Create folders
RUN mkdir -p /opt/airflow/dags \
  && mkdir -p mutif-etl-folder \
  && mkdir -p mutif-etl-folder/increment-session \
  && mkdir -p mutif-etl-folder/mutif_etl \
  && mkdir -p mutif-etl-folder/queries

# Copy files
COPY environment.py mutif-etl-folder
COPY queries/* mutif-etl-folder/queries
COPY mutif_etl/* mutif-etl-folder/mutif_etl
COPY increment-session/* mutif-etl-folder/increment-session
COPY DAGs/* /opt/airflow/dags

# Create Volume
VOLUME mutif-etl-folder

# Expose port
EXPOSE 8080

# Run Airflow
CMD airflow db migrate
CMD airflow standalone
