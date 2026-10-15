import sqlite3

with sqlite3.connect("SQLData/taxi.db") as connection:
    result = connection.execute(
        """
        SELECT
            MIN(tpep_pickup_datetime),
            MAX(tpep_pickup_datetime),

            SUM(
                CASE
                    WHEN tpep_pickup_datetime < '2024-01-01'
                      OR tpep_pickup_datetime >= '2024-02-01'
                    THEN 1
                    ELSE 0
                END
            ) AS invalid_pickup_dates

        FROM taxi_trips
        """
    ).fetchone()

    print("Minimum pickup:", result[0])
    print("Maximum pickup:", result[1])
    print("Invalid pickup dates:", result[2])