FROM quay.io/astronomer/astro-runtime:10.5.0-python-3.10

USER root

RUN curl https://packages.microsoft.com/keys/microsoft.asc | sudo tee /etc/apt/trusted.gpg.d/microsoft.asc \
    && curl https://packages.microsoft.com/config/debian/11/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list \
    && echo 'debconf debconf/frontend select Noninteractive' | debconf-set-selections \
    && sudo apt-get install -y -q \
    && sudo apt-get update \
    && sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && sudo ACCEPT_EULA=Y apt-get install -y mssql-tools18 \
    && echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc \
    && source ~/.bashrc \
    && chmod +rwx /etc/ssl/openssl.cnf \
    && sed -i 's/TLSv1.2/TLSv1/g' /etc/ssl/openssl.cnf \
    && sed -i 's/SECLEVEL=2/SECLEVEL=1/g' /etc/ssl/openssl.cnf \
    && python -m venv dbt_venv \
    && source dbt_venv/bin/activate \
    && pip install --no-cache-dir dbt-snowflake protobuf==4.25.3 \
    && deactivate
USER airflow

