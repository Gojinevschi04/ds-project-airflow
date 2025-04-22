import logging
import os
import pickle
from datetime import datetime
from typing import Any

import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from helpers.db import select
from operators.db_insert import DbInsertOperator
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split

MODEL_PATH = "/opt/airflow/data/models/model_regression_1.pkl"
COUNTRY_ID = 1
TARGET = "active"
FEATURE_COLUMNS = ["confirmed", "deaths",
                   "average_temperature_c", "average_humidity", "total_precipitation_mm"
                   ]


@dag(
    schedule_interval="@daily",
    start_date=datetime(2025, 3, 1),
    template_searchpath="/opt/project/",
    catchup=False,
    tags=["ml"],
)
def ML() -> None:
    init = SQLExecuteQueryOperator(
        task_id="init_visualization_tables",
        conn_id="pg_conn",
        sql=[
            "sql/create_visualization_coefficients_table.sql",
            "sql/create_visualization_covid_weather_fact_table.sql",
        ],
    )

    @task()
    def extract_data() -> list[dict[str, Any]]:
        data = select("pg_conn", "dbo.dw_covid_weather_fact")
        columns = ["unique_id", "country_id", "date", "confirmed", "deaths", "recovered",
                   "active",
                   "fatality_rate", "average_temperature_c", "average_humidity",
                   "total_precipitation_mm"]
        df = pd.DataFrame(data, columns=columns)
        return df.to_dict("records")

    @task()
    def train_model(data_dict, country_id: int = None) -> None:
        df = pd.DataFrame.from_dict(data_dict)

        if country_id is not None:
            df = df[df["country_id"] == country_id]

        X = df[FEATURE_COLUMNS]

        y = df[TARGET]

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )

        model = LinearRegression()
        model.fit(X_train, y_train)

        y_pred = model.predict(X_test)
        # acc = accuracy_score(y_test, y_pred)
        # logging.info(f"Model accuracy: {acc:.2f}")

        mse = mean_squared_error(y_test, y_pred)
        logging.info(f"MSE: {mse:.2f}")

        with open(MODEL_PATH, "wb") as f:
            pickle.dump(model, f)

        logging.info("Model trained and saved")

    @task()
    def predict(data_dict, country_id: int = None, **kwargs) -> None:
        df = pd.DataFrame.from_dict(data_dict)

        if country_id is not None:
            df = df[df["country_id"] == country_id]

        with open(MODEL_PATH, "rb") as f:
            model = pickle.load(f)

        columns = ["unique_id", "country_id", "date"]

        df["prediction"] = model.predict(df[FEATURE_COLUMNS])

        result_df = df[columns + ["prediction"]]
        result_df["real_value"] = df[TARGET]

        kwargs["ti"].xcom_push("predictions", result_df.to_dict("records"))
        logging.info("Predictions saved")

    @task()
    def view_model(**kwargs) -> None:
        if not os.path.exists(MODEL_PATH):
            logging.error(f"Model file not found at: {MODEL_PATH}")
            return

        with open(MODEL_PATH, "rb") as f:
            model = pickle.load(f)

        logging.info("Model loaded successfully!")
        logging.info("Model details:")
        logging.info(model)

        if hasattr(model, "coef_"):
            coef_df = pd.DataFrame({
                "name": FEATURE_COLUMNS,
                "value": model.coef_,
            })
            logging.info("Coefficients:")
            logging.info(coef_df.T)

            kwargs["ti"].xcom_push("coefficients", coef_df.to_dict("records"))
        else:
            logging.error("Model does not have coef_ attribute (maybe not a linear model).")

        # if hasattr(model, "intercept_"):
        #     logging.error("Intercept:", model.intercept_)

    insert_coefficients = DbInsertOperator(
        task_id="insert_coefficients",
        ti_id="view_model",
        ti_key="coefficients",
        table="visualization_coefficients",
        postgres_conn_id="pg_conn",
    )

    insert_predictions = DbInsertOperator(
        task_id="insert_predictions",
        ti_id="predict",
        ti_key="predictions",
        table="visualization_covid_weather_fact",
        # unique_columns=["date", "country_id"],
        postgres_conn_id="pg_conn",
    )

    data = extract_data()
    train_model(data, country_id=COUNTRY_ID)
    init >> predict(data, country_id=COUNTRY_ID) >> insert_predictions >> view_model() >> insert_coefficients


dag = ML()

if __name__ == "__main__":
    dag.test()
