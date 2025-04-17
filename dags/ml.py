import logging
import os
import pickle
from datetime import datetime
from typing import Any

import pandas as pd
from airflow.decorators import dag, task
from helpers.db import select
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split

MODEL_PATH = "/opt/airflow/data/models/model.pkl"


@dag(schedule_interval="@daily", start_date=datetime(2025, 3, 1), catchup=False, tags=["ml"])
def ML() -> None:
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
    def train_model(data_dict) -> None:
        df = pd.DataFrame.from_dict(data_dict)

        df["target"] = (df["fatality_rate"] >= 0.02).astype(int)

        X = df[
            [
                "confirmed",
                "recovered",
                "deaths",
                "active",
                "average_temperature_c",
                "average_humidity",
                "total_precipitation_mm",
            ]
        ]
        y = df["target"]

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )

        model = LogisticRegression(max_iter=1000)
        model.fit(X_train, y_train)

        y_pred = model.predict(X_test)
        acc = accuracy_score(y_test, y_pred)
        logging.info(f"Model accuracy: {acc:.2f}")

        with open(MODEL_PATH, "wb") as f:
            pickle.dump(model, f)

        return "Model trained and saved"

    @task()
    def predict(data_dict) -> None:
        df = pd.DataFrame.from_dict(data_dict)

        features = df[
            [
                "confirmed",
                "recovered",
                "deaths",
                "active",
                "average_temperature_c",
                "average_humidity",
                "total_precipitation_mm",
            ]
        ]

        with open(MODEL_PATH, "rb") as f:
            model = pickle.load(f)

        df["prediction"] = model.predict(features)

        result_df = df[
            ["unique_id", "country_id", "date", "fatality_rate", "prediction"]
        ]

        # engine = create_engine(DB_URI)
        # result_df.to_sql("prediction_results", engine, if_exists="replace", index=False)

        return "Predictions saved"

    def view_model() -> None:
        if not os.path.exists(MODEL_PATH):
            print(f"Model file not found at: {MODEL_PATH}")
            return

        with open(MODEL_PATH, "rb") as f:
            model = pickle.load(f)

        logging.info("Model loaded successfully!")
        logging.info("Model details:")
        logging.info(model)

        if hasattr(model, "coef_"):
            feature_names = [
                "confirmed",
                "deaths",
                "recovered",
                "active",
                "temperature",
                "humidity",
                "precipitation",
            ]
            coef_df = pd.DataFrame(model.coef_, columns=feature_names)
            logging.info("Coefficients:")
            logging.info(coef_df.T)
        else:
            logging.error("Model does not have coef_ attribute (maybe not a linear model).")

        if hasattr(model, "intercept_"):
            logging.error("Intercept:", model.intercept_)

    data = extract_data()
    train_model(data) >> predict(data) >> view_model()


dag = ML()

if __name__ == "__main__":
    dag.test()
