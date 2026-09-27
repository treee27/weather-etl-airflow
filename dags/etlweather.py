from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from datetime import datetime

LATITUDE = "51.5074"
LONGITUDE = "-0.1278"

POSTGRES_CONN_ID = "postgres_default"
API_CONN_ID = "open_meteo_api"


default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 9, 26),
}


with DAG(
    dag_id="weather_etl_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
) as dag:

    @task
    def extract_weather_data():

        http_hook = HttpHook(
            method="GET",
            http_conn_id=API_CONN_ID
        )

        endpoint = (
            f"/v1/forecast"
            f"?latitude={LATITUDE}"
            f"&longitude={LONGITUDE}"
            f"&hourly=temperature_2m,windspeed_10m,winddirection_10m,weathercode"
        )

        response = http_hook.run(endpoint)

        return response.json()


    @task
    def transform_weather_data(weather_data):

        hourly_data = weather_data.get("hourly", {})

        times = hourly_data.get("time", [])
        temperatures = hourly_data.get("temperature_2m", [])
        windspeeds = hourly_data.get("windspeed_10m", [])
        winddirections = hourly_data.get("winddirection_10m", [])
        weathercodes = hourly_data.get("weathercode", [])

        transformed_data = []

        for i in range(len(times)):

            record = {
                "latitude": weather_data["latitude"],
                "longitude": weather_data["longitude"],
                "timestamp": times[i],
                "temperature": temperatures[i],
                "windspeed": windspeeds[i],
                "winddirection": winddirections[i],
                "weathercode": weathercodes[i],
            }

            transformed_data.append(record)

        return transformed_data


    @task
    def load_weather_data(transformed_data):

        postgres_hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )

        conn = postgres_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weatherdata (
                latitude FLOAT,
                longitude FLOAT,
                temperature FLOAT,
                windspeed FLOAT,
                winddirection FLOAT,
                weathercode INT,
                timestamp TIMESTAMP
            );
        """)

        insert_query = """
            INSERT INTO weatherdata (
                latitude,
                longitude,
                temperature,
                windspeed,
                winddirection,
                weathercode,
                timestamp
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        for record in transformed_data:

            cursor.execute(
                insert_query,
                (
                    record["latitude"],
                    record["longitude"],
                    record["temperature"],
                    record["windspeed"],
                    record["winddirection"],
                    record["weathercode"],
                    record["timestamp"],
                )
            )

        conn.commit()

        cursor.close()
        conn.close()


    weather_data = extract_weather_data()

    transformed_data = transform_weather_data(
        weather_data
    )

    load_weather_data(
        transformed_data
    )