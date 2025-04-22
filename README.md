# DS Project Airflow

This project implements an automated ETL pipeline using Apache Airflow to collect and process weather and COVID-19 data
from public APIs. The pipeline handles data extraction, transformation, loading, and analysis, with support for
reporting and optional machine learning integration.

## Installation

### Before start

- Install [docker](https://docs.docker.com/get-started/get-docker/)
- Install [docker-compose](https://docs.docker.com/compose/install/)

### Setup

Init env file

```bash
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

Run database migrations and create the first user account.

```bash
docker compose up airflow-init
```

Run airflow

```bash
docker compose up
```

## Usage

### Run CLI commands

Via docker compose

```bash
docker compose run airflow-worker airflow info
```

Via sh script

```bash
./airflow.sh info
```

```bash
./airflow.sh bash
```

```bash
./airflow.sh python
```

### Accessing the web interface

The webserver is available at: `http://localhost:8080`.
The default account has the login `airflow` and the password
`airflow`.

### Creating default db connection

Complete the form `http://localhost:8080/connection/add/` with
Connection Id: `pg_conn`
Connection Type: `postgres`
Host: `postgres`
Schema: `airflow`
Login: `airflow`
Password: `airflow`
Port: `5432`

### Superset Reporting

The Superset web interface is available at:
`http://localhost:8088`.
Login credentials (default):
Username: `airflow`
Password: `airflow`

### Connecting to PostgreSQL

Go to Data → Databases → + Database.
Select PostgreSQL.
Fill in the fields:
SQLAlchemy URI:
`postgresql+psycopg2://airflow:airflow@postgres:5432/airflow`
Click Test Connection and then Connect.

You can now explore tables like covid, weather, and dw_covid_weather_fact, and build visual dashboards for reporting.