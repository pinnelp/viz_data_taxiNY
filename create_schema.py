import sqlite3
from pathlib import Path


DATABASE_PATH = "SQLData/taxi.db"


# ==================================================
# General checks
# ==================================================

def check_source_tables(
    connection: sqlite3.Connection
) -> None:
    """Confirm that the required ETL source tables exist."""

    required_tables = {
        "taxi_trips",
        "taxi_zones"
    }

    existing_tables = {
        row[0]
        for row in connection.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table'
            """
        ).fetchall()
    }

    missing_tables = required_tables - existing_tables

    if missing_tables:
        raise RuntimeError(
            "Missing source tables: "
            + ", ".join(sorted(missing_tables))
        )

    print("Source tables found.")


def drop_existing_schema(
    connection: sqlite3.Connection
) -> None:
    """Remove existing fact and dimension tables."""

    tables = [
        "FactTrips",
        "DimTaxiZone",
        "DimVendor",
        "DimRateCode",
        "DimPaymentType",
        "DimDate",
        "DimTime",
        "DimBorough",
        "DimServiceZone"
    ]

    for table_name in tables:
        connection.execute(
            f"DROP TABLE IF EXISTS {table_name}"
        )

    connection.commit()

    print("Existing analytical schema removed.")


# ==================================================
# Borough and service-zone dimensions
# ==================================================

def create_location_dimensions(
    connection: sqlite3.Connection
) -> None:
    """Create DimBorough and DimServiceZone."""

    connection.executescript(
        """
        CREATE TABLE DimBorough (
            BoroughKey INTEGER PRIMARY KEY AUTOINCREMENT,
            BoroughName TEXT NOT NULL UNIQUE
        );

        CREATE TABLE DimServiceZone (
            ServiceZoneKey INTEGER PRIMARY KEY AUTOINCREMENT,
            ServiceZoneName TEXT NOT NULL UNIQUE
        );

        INSERT INTO DimBorough (
            BoroughKey,
            BoroughName
        )
        VALUES (
            0,
            'Unknown'
        );

        INSERT INTO DimServiceZone (
            ServiceZoneKey,
            ServiceZoneName
        )
        VALUES (
            0,
            'Unknown'
        );

        INSERT INTO DimBorough (
            BoroughName
        )
        SELECT DISTINCT
            TRIM(Borough)
        FROM taxi_zones
        WHERE Borough IS NOT NULL
          AND TRIM(Borough) <> ''
          AND TRIM(Borough) <> 'Unknown'
        ORDER BY TRIM(Borough);

        INSERT INTO DimServiceZone (
            ServiceZoneName
        )
        SELECT DISTINCT
            TRIM(service_zone)
        FROM taxi_zones
        WHERE service_zone IS NOT NULL
          AND TRIM(service_zone) <> ''
          AND TRIM(service_zone) <> 'Unknown'
        ORDER BY TRIM(service_zone);
        """
    )

    connection.commit()

    print("DimBorough and DimServiceZone created.")


# ==================================================
# Taxi-zone dimension
# ==================================================

def create_taxi_zone_dimension(
    connection: sqlite3.Connection
) -> None:
    """Create DimTaxiZone."""

    connection.executescript(
        """
        CREATE TABLE DimTaxiZone (
            LocationID INTEGER PRIMARY KEY,
            ZoneName TEXT NOT NULL,
            BoroughKey INTEGER NOT NULL,
            ServiceZoneKey INTEGER NOT NULL,

            FOREIGN KEY (BoroughKey)
                REFERENCES DimBorough (BoroughKey),

            FOREIGN KEY (ServiceZoneKey)
                REFERENCES DimServiceZone (ServiceZoneKey)
        );

        INSERT INTO DimTaxiZone (
            LocationID,
            ZoneName,
            BoroughKey,
            ServiceZoneKey
        )
        SELECT
            CAST(zones.LocationID AS INTEGER),

            COALESCE(
                NULLIF(TRIM(zones.Zone), ''),
                'Unknown'
            ),

            COALESCE(
                borough.BoroughKey,
                0
            ),

            COALESCE(
                service_zone.ServiceZoneKey,
                0
            )

        FROM taxi_zones AS zones

        LEFT JOIN DimBorough AS borough
            ON borough.BoroughName =
               TRIM(zones.Borough)

        LEFT JOIN DimServiceZone AS service_zone
            ON service_zone.ServiceZoneName =
               TRIM(zones.service_zone)

        WHERE zones.LocationID IS NOT NULL;

        CREATE INDEX idx_taxi_zone_borough
        ON DimTaxiZone (BoroughKey);

        CREATE INDEX idx_taxi_zone_service
        ON DimTaxiZone (ServiceZoneKey);
        """
    )

    connection.commit()

    print("DimTaxiZone created.")


# ==================================================
# Vendor dimension
# ==================================================

def create_vendor_dimension(
    connection: sqlite3.Connection
) -> None:
    """Create DimVendor using fixed lookup values."""

    connection.execute(
        """
        CREATE TABLE DimVendor (
            VendorKey INTEGER PRIMARY KEY,
            VendorID INTEGER UNIQUE,
            VendorName TEXT NOT NULL
        )
        """
    )

    vendor_rows = [
        (0, None, "Unknown"),
        (1, 1, "Creative Mobile Technologies"),
        (2, 2, "VeriFone"),
        (3, 6, "Unmapped vendor 6"),
        (4, 7, "Unmapped vendor 7")
    ]

    connection.executemany(
        """
        INSERT INTO DimVendor (
            VendorKey,
            VendorID,
            VendorName
        )
        VALUES (?, ?, ?)
        """,
        vendor_rows
    )

    connection.commit()

    print("DimVendor created.")


# ==================================================
# Rate-code dimension
# ==================================================

def create_rate_code_dimension(
    connection: sqlite3.Connection
) -> None:
    """Create DimRateCode using fixed lookup values."""

    connection.execute(
        """
        CREATE TABLE DimRateCode (
            RateCodeKey INTEGER PRIMARY KEY,
            RatecodeID INTEGER UNIQUE,
            RateCodeName TEXT NOT NULL
        )
        """
    )

    rate_code_rows = [
        (0, None, "Unknown"),
        (1, 1, "Standard rate"),
        (2, 2, "JFK"),
        (3, 3, "Newark"),
        (4, 4, "Nassau or Westchester"),
        (5, 5, "Negotiated fare"),
        (6, 6, "Group ride"),
        (7, 99, "Unknown rate")
    ]

    connection.executemany(
        """
        INSERT INTO DimRateCode (
            RateCodeKey,
            RatecodeID,
            RateCodeName
        )
        VALUES (?, ?, ?)
        """,
        rate_code_rows
    )

    connection.commit()

    print("DimRateCode created.")


# ==================================================
# Payment-type dimension
# ==================================================

def create_payment_type_dimension(
    connection: sqlite3.Connection
) -> None:
    """Create DimPaymentType using fixed lookup values."""

    connection.execute(
        """
        CREATE TABLE DimPaymentType (
            PaymentTypeKey INTEGER PRIMARY KEY,
            PaymentTypeID INTEGER UNIQUE,
            PaymentTypeName TEXT NOT NULL
        )
        """
    )

    payment_rows = [
        (0, None, "Unknown"),
        (1, 0, "Unknown payment"),
        (2, 1, "Credit card"),
        (3, 2, "Cash"),
        (4, 3, "No charge"),
        (5, 4, "Dispute"),
        (6, 5, "Unknown"),
        (7, 6, "Voided trip")
    ]

    connection.executemany(
        """
        INSERT INTO DimPaymentType (
            PaymentTypeKey,
            PaymentTypeID,
            PaymentTypeName
        )
        VALUES (?, ?, ?)
        """,
        payment_rows
    )

    connection.commit()

    print("DimPaymentType created.")


def create_trip_dimensions(
    connection: sqlite3.Connection
) -> None:
    """Create all categorical trip dimensions."""

    create_vendor_dimension(connection)
    create_rate_code_dimension(connection)
    create_payment_type_dimension(connection)


# ==================================================
# Date dimension
# ==================================================

def create_date_dimension(
    connection: sqlite3.Connection
) -> None:
    """Create DimDate from valid trip timestamps."""

    connection.execute(
        """
        CREATE TABLE DimDate (
            DateKey INTEGER PRIMARY KEY,
            FullDate TEXT NOT NULL UNIQUE,
            Year INTEGER NOT NULL,
            Quarter INTEGER NOT NULL,
            MonthNumber INTEGER NOT NULL,
            MonthName TEXT NOT NULL,
            DayOfMonth INTEGER NOT NULL,
            DayOfWeekNumber INTEGER NOT NULL,
            DayOfWeekName TEXT NOT NULL,
            IsWeekend INTEGER NOT NULL
        )
        """
    )

    connection.execute(
        """
        INSERT INTO DimDate (
            DateKey,
            FullDate,
            Year,
            Quarter,
            MonthNumber,
            MonthName,
            DayOfMonth,
            DayOfWeekNumber,
            DayOfWeekName,
            IsWeekend
        )
        WITH AllDates AS (
            SELECT
                DATE(tpep_pickup_datetime) AS FullDate
            FROM taxi_trips

            UNION

            SELECT
                DATE(tpep_dropoff_datetime) AS FullDate
            FROM taxi_trips
        )
        SELECT
            CAST(
                STRFTIME('%Y%m%d', FullDate)
                AS INTEGER
            ),

            FullDate,

            CAST(
                STRFTIME('%Y', FullDate)
                AS INTEGER
            ),

            (
                (
                    CAST(
                        STRFTIME('%m', FullDate)
                        AS INTEGER
                    ) - 1
                ) / 3
            ) + 1,

            CAST(
                STRFTIME('%m', FullDate)
                AS INTEGER
            ),

            CASE STRFTIME('%m', FullDate)
                WHEN '01' THEN 'January'
                WHEN '02' THEN 'February'
                WHEN '03' THEN 'March'
                WHEN '04' THEN 'April'
                WHEN '05' THEN 'May'
                WHEN '06' THEN 'June'
                WHEN '07' THEN 'July'
                WHEN '08' THEN 'August'
                WHEN '09' THEN 'September'
                WHEN '10' THEN 'October'
                WHEN '11' THEN 'November'
                WHEN '12' THEN 'December'
            END,

            CAST(
                STRFTIME('%d', FullDate)
                AS INTEGER
            ),

            CASE STRFTIME('%w', FullDate)
                WHEN '0' THEN 7
                ELSE CAST(
                    STRFTIME('%w', FullDate)
                    AS INTEGER
                )
            END,

            CASE STRFTIME('%w', FullDate)
                WHEN '0' THEN 'Sunday'
                WHEN '1' THEN 'Monday'
                WHEN '2' THEN 'Tuesday'
                WHEN '3' THEN 'Wednesday'
                WHEN '4' THEN 'Thursday'
                WHEN '5' THEN 'Friday'
                WHEN '6' THEN 'Saturday'
            END,

            CASE
                WHEN STRFTIME('%w', FullDate)
                     IN ('0', '6')
                    THEN 1
                ELSE 0
            END

        FROM AllDates

        WHERE FullDate IS NOT NULL

        ORDER BY FullDate
        """
    )

    connection.commit()

    print("DimDate created.")


# ==================================================
# Time dimension
# ==================================================

def create_time_dimension(
    connection: sqlite3.Connection
) -> None:
    """Create DimTime from trip timestamps."""

    connection.execute(
        """
        CREATE TABLE DimTime (
            TimeKey INTEGER PRIMARY KEY,
            FullTime TEXT NOT NULL UNIQUE,
            Hour INTEGER NOT NULL,
            Minute INTEGER NOT NULL,
            Second INTEGER NOT NULL,
            TimePeriod TEXT NOT NULL
        )
        """
    )

    connection.execute(
        """
        INSERT INTO DimTime (
            TimeKey,
            FullTime,
            Hour,
            Minute,
            Second,
            TimePeriod
        )
        WITH AllTimes AS (
            SELECT
                TIME(tpep_pickup_datetime) AS FullTime
            FROM taxi_trips

            UNION

            SELECT
                TIME(tpep_dropoff_datetime) AS FullTime
            FROM taxi_trips
        )
        SELECT
            CAST(
                STRFTIME('%H%M%S', FullTime)
                AS INTEGER
            ),

            FullTime,

            CAST(
                STRFTIME('%H', FullTime)
                AS INTEGER
            ),

            CAST(
                STRFTIME('%M', FullTime)
                AS INTEGER
            ),

            CAST(
                STRFTIME('%S', FullTime)
                AS INTEGER
            ),

            CASE
                WHEN CAST(
                    STRFTIME('%H', FullTime)
                    AS INTEGER
                ) BETWEEN 5 AND 11
                    THEN 'Morning'

                WHEN CAST(
                    STRFTIME('%H', FullTime)
                    AS INTEGER
                ) BETWEEN 12 AND 16
                    THEN 'Afternoon'

                WHEN CAST(
                    STRFTIME('%H', FullTime)
                    AS INTEGER
                ) BETWEEN 17 AND 21
                    THEN 'Evening'

                ELSE 'Night'
            END

        FROM AllTimes

        WHERE FullTime IS NOT NULL

        ORDER BY FullTime
        """
    )

    connection.commit()

    print("DimTime created.")


# ==================================================
# Fact table
# ==================================================

def create_fact_trips(
    connection: sqlite3.Connection
) -> None:
    """Create and populate FactTrips."""

    connection.execute(
        """
        CREATE TABLE FactTrips (
            TripKey INTEGER PRIMARY KEY AUTOINCREMENT,
            RecordID TEXT NOT NULL UNIQUE,

            VendorKey INTEGER NOT NULL,
            RateCodeKey INTEGER NOT NULL,
            PaymentTypeKey INTEGER NOT NULL,

            PickupDateKey INTEGER NOT NULL,
            PickupTimeKey INTEGER NOT NULL,
            DropoffDateKey INTEGER NOT NULL,
            DropoffTimeKey INTEGER NOT NULL,

            PickupLocationID INTEGER NOT NULL,
            DropoffLocationID INTEGER NOT NULL,

            PassengerCount INTEGER,
            TripDistance REAL,
            TripDurationMinutes REAL NOT NULL,

            StoreAndForwardFlag TEXT,

            FareAmount REAL,
            ExtraAmount REAL,
            MtaTax REAL,
            TipAmount REAL,
            TollsAmount REAL,
            ImprovementSurcharge REAL,
            TotalAmount REAL,
            CongestionSurcharge REAL,
            AirportFee REAL,

            FOREIGN KEY (VendorKey)
                REFERENCES DimVendor (VendorKey),

            FOREIGN KEY (RateCodeKey)
                REFERENCES DimRateCode (RateCodeKey),

            FOREIGN KEY (PaymentTypeKey)
                REFERENCES DimPaymentType (PaymentTypeKey),

            FOREIGN KEY (PickupDateKey)
                REFERENCES DimDate (DateKey),

            FOREIGN KEY (PickupTimeKey)
                REFERENCES DimTime (TimeKey),

            FOREIGN KEY (DropoffDateKey)
                REFERENCES DimDate (DateKey),

            FOREIGN KEY (DropoffTimeKey)
                REFERENCES DimTime (TimeKey),

            FOREIGN KEY (PickupLocationID)
                REFERENCES DimTaxiZone (LocationID),

            FOREIGN KEY (DropoffLocationID)
                REFERENCES DimTaxiZone (LocationID)
        )
        """
    )

    print("Creating FactTrips...")

    connection.execute(
        """
        INSERT INTO FactTrips (
            RecordID,
            VendorKey,
            RateCodeKey,
            PaymentTypeKey,
            PickupDateKey,
            PickupTimeKey,
            DropoffDateKey,
            DropoffTimeKey,
            PickupLocationID,
            DropoffLocationID,
            PassengerCount,
            TripDistance,
            TripDurationMinutes,
            StoreAndForwardFlag,
            FareAmount,
            ExtraAmount,
            MtaTax,
            TipAmount,
            TollsAmount,
            ImprovementSurcharge,
            TotalAmount,
            CongestionSurcharge,
            AirportFee
        )
        SELECT
            trips._record_id,

            COALESCE(
                vendor.VendorKey,
                0
            ),

            COALESCE(
                rate_code.RateCodeKey,
                0
            ),

            COALESCE(
                payment.PaymentTypeKey,
                0
            ),

            CAST(
                STRFTIME(
                    '%Y%m%d',
                    trips.tpep_pickup_datetime
                )
                AS INTEGER
            ),

            CAST(
                STRFTIME(
                    '%H%M%S',
                    trips.tpep_pickup_datetime
                )
                AS INTEGER
            ),

            CAST(
                STRFTIME(
                    '%Y%m%d',
                    trips.tpep_dropoff_datetime
                )
                AS INTEGER
            ),

            CAST(
                STRFTIME(
                    '%H%M%S',
                    trips.tpep_dropoff_datetime
                )
                AS INTEGER
            ),

            CAST(
                trips.PULocationID
                AS INTEGER
            ),

            CAST(
                trips.DOLocationID
                AS INTEGER
            ),

            CASE
                WHEN trips.passenger_count IS NULL
                    THEN NULL
                ELSE CAST(
                    trips.passenger_count
                    AS INTEGER
                )
            END,

            trips.trip_distance,

            ROUND(
                (
                    JULIANDAY(
                        trips.tpep_dropoff_datetime
                    )
                    -
                    JULIANDAY(
                        trips.tpep_pickup_datetime
                    )
                ) * 24 * 60,
                2
            ),

            trips.store_and_fwd_flag,
            trips.fare_amount,
            trips.extra,
            trips.mta_tax,
            trips.tip_amount,
            trips.tolls_amount,
            trips.improvement_surcharge,
            trips.total_amount,
            trips.congestion_surcharge,
            trips.Airport_fee

        FROM taxi_trips AS trips

        LEFT JOIN DimVendor AS vendor
            ON vendor.VendorID =
               trips.VendorID

        LEFT JOIN DimRateCode AS rate_code
            ON rate_code.RatecodeID =
               trips.RatecodeID

        LEFT JOIN DimPaymentType AS payment
            ON payment.PaymentTypeID =
               trips.payment_type
        """
    )

    connection.commit()

    print("FactTrips data inserted.")


# ==================================================
# Fact indexes
# ==================================================

def create_fact_indexes(
    connection: sqlite3.Connection
) -> None:
    """Create indexes for relationships and filtering."""

    indexes = {
        "idx_fact_vendor": "VendorKey",
        "idx_fact_rate_code": "RateCodeKey",
        "idx_fact_payment": "PaymentTypeKey",
        "idx_fact_pickup_date": "PickupDateKey",
        "idx_fact_dropoff_date": "DropoffDateKey",
        "idx_fact_pickup_time": "PickupTimeKey",
        "idx_fact_dropoff_time": "DropoffTimeKey",
        "idx_fact_pickup_location": "PickupLocationID",
        "idx_fact_dropoff_location": "DropoffLocationID"
    }

    print("Creating FactTrips indexes...")

    for index_name, column_name in indexes.items():
        connection.execute(
            f"""
            CREATE INDEX {index_name}
            ON FactTrips ({column_name})
            """
        )

    connection.commit()

    print("FactTrips indexes created.")


# ==================================================
# Validation
# ==================================================

def verify_dimension_counts(
    connection: sqlite3.Connection
) -> None:
    """Display dimension row counts."""

    dimension_tables = [
        "DimBorough",
        "DimServiceZone",
        "DimTaxiZone",
        "DimVendor",
        "DimRateCode",
        "DimPaymentType",
        "DimDate",
        "DimTime"
    ]

    print("\nDimension row counts:")

    for table_name in dimension_tables:
        count = connection.execute(
            f"SELECT COUNT(*) FROM {table_name}"
        ).fetchone()[0]

        print(f"{table_name}: {count:,}")


def verify_fact_trips(
    connection: sqlite3.Connection
) -> None:
    """Verify FactTrips row counts and relationships."""

    source_count = connection.execute(
        "SELECT COUNT(*) FROM taxi_trips"
    ).fetchone()[0]

    fact_count = connection.execute(
        "SELECT COUNT(*) FROM FactTrips"
    ).fetchone()[0]

    duplicate_count = connection.execute(
        """
        SELECT
            COUNT(*) - COUNT(DISTINCT RecordID)
        FROM FactTrips
        """
    ).fetchone()[0]

    invalid_duration_count = connection.execute(
        """
        SELECT COUNT(*)
        FROM FactTrips
        WHERE TripDurationMinutes < 0
           OR TripDurationMinutes > 1440
        """
    ).fetchone()[0]

    foreign_key_errors = connection.execute(
        "PRAGMA foreign_key_check(FactTrips)"
    ).fetchall()

    print("\nFactTrips validation:")
    print(f"taxi_trips rows: {source_count:,}")
    print(f"FactTrips rows: {fact_count:,}")
    print(f"Row counts match: {source_count == fact_count}")
    print(f"Duplicate RecordIDs: {duplicate_count:,}")
    print(
        "Invalid trip durations: "
        f"{invalid_duration_count:,}"
    )
    print(
        "Foreign-key errors: "
        f"{len(foreign_key_errors):,}"
    )


def show_fact_sample(
    connection: sqlite3.Connection
) -> None:
    """Display a small sample from FactTrips."""

    rows = connection.execute(
        """
        SELECT
            TripKey,
            PickupDateKey,
            PickupTimeKey,
            PickupLocationID,
            DropoffLocationID,
            PassengerCount,
            TripDistance,
            TripDurationMinutes,
            TotalAmount

        FROM FactTrips

        ORDER BY TripKey

        LIMIT 10
        """
    ).fetchall()

    print("\nFactTrips sample:")

    for row in rows:
        print(row)


# ==================================================
# Main
# ==================================================

def main() -> None:
    """Create and validate the complete snowflake schema."""

    database_file = Path(DATABASE_PATH)

    if not database_file.exists():
        raise FileNotFoundError(
            f"Database not found: {DATABASE_PATH}"
        )

    with sqlite3.connect(DATABASE_PATH) as connection:
        connection.execute(
            "PRAGMA foreign_keys = ON"
        )

        check_source_tables(connection)
        drop_existing_schema(connection)

        create_location_dimensions(connection)
        create_taxi_zone_dimension(connection)

        create_trip_dimensions(connection)

        create_date_dimension(connection)
        create_time_dimension(connection)

        create_fact_trips(connection)
        create_fact_indexes(connection)

        verify_dimension_counts(connection)
        verify_fact_trips(connection)
        show_fact_sample(connection)

    print("\nSnowflake schema created successfully.")


if __name__ == "__main__":
    main()