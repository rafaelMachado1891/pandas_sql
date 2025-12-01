FROM quay.io/astronomer/astro-runtime:10.0.0

USER root

RUN apt-get update \
    && apt-get install -y gnupg curl apt-transport-https \
    && curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /etc/apt/trusted.gpg.d/microsoft.gpg \
    && curl https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 mssql-tools18 unixodbc-dev gcc g++ \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER astro

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


