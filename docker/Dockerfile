# Airflow with the platform installed. Not built or run by me: see the README.
FROM apache/airflow:3.4.0-python3.12
COPY --chown=airflow:root pyproject.toml README.md /opt/platform/
COPY --chown=airflow:root src /opt/platform/src
RUN pip install --no-cache-dir "/opt/platform[postgres,cloud]"
ENV PLATFORM_ROOT=/opt/platform
COPY --chown=airflow:root pipelines /opt/platform/pipelines
COPY --chown=airflow:root config /opt/platform/config
COPY --chown=airflow:root dags /opt/airflow/dags
