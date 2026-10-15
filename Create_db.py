import os
from dotenv import load_dotenv
import json
import sqlite3
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq


# --------------------------------------------------
# File configuration
# --------------------------------------------------

load_dotenv()

MAIN_PATH = os.getenv("MAIN_PATH")

if not MAIN_PATH:
    raise ValueError(
        "MAIN_PATH is missing from the .env file."
    )

TRIPS_PATH = f"{MAIN_PATH}/RawData/yellow_tripdata_2024-01.parquet"
ZONES_PATH = f"{MAIN_PATH}/RawData/taxi_zone_lookup.csv"
DATABASE_PATH = f"{MAIN_PATH}/SQLData/taxi.db"
INVALID_DATABASE_PATH = f"{MAIN_PATH}/SQLData/inc_Data_taxi.db"
AUDIT_PATH = f"{MAIN_PATH}/audit/data_quality_report.json"

BATCH_SIZE = 200_000

REPORTING_PERIOD_START = pd.Timestamp("2024-01-01")
REPORTING_PERIOD_END = pd.Timestamp("2024-02-01")

MAXIMUM_TRIP_DURATION = pd.Timedelta(hours=24)

# --------------------------------------------------
# Data-cleaning configuration
# --------------------------------------------------

DATETIME_COLUMNS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

NUMERIC_COLUMNS = [
    "VendorID",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "Airport_fee"
]

# Only these columns will have negative values converted with abs().
POSITIVE_VALUE_COLUMNS = [
    "trip_distance",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "Airport_fee"
]

# Missing values in these optional columns become zero.
OPTIONAL_CHARGE_COLUMNS = [
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "congestion_surcharge",
    "Airport_fee"
]

# Allowed categorical codes.
VALID_CATEGORY_VALUES = {
    "VendorID": {1, 2, 6, 7},
    "RatecodeID": {1, 2, 3, 4, 5, 6, 99},
    "payment_type": {0, 1, 2, 3, 4, 5, 6}
}


# --------------------------------------------------
# Audit functions
# --------------------------------------------------

def create_audit_report() -> dict:
    """Create an empty audit report."""



    return {
        "raw": {
            "rows": 0,
            "missing_values": {},
            "duplicate_rows": 0,
            "invalid_timestamps": 0,
            "invalid_trip_duration": 0,
            "invalid_distance": 0,
            "invalid_fare": 0,
            "invalid_total_amount": 0,
            "invalid_location_id": 0,
            "invalid_reporting_period": 0,
        },
        "processing": {
            "negative_values_corrected": {},
            "missing_values_replaced": {},
            "category_values_replaced": {},
            "passenger_counts_replaced": 0,
            "rejected_rows": 0,
            "duplicate_rows_removed": 0,
            "rows_sent_to_database": 0
        },
        "database": {},
        "validation": {}
    }


def update_audit_counter(
    audit_section: dict,
    key: str,
    amount: int
) -> None:
    """Add an amount to an audit counter."""

    audit_section[key] = (
        audit_section.get(key, 0) + amount
    )


def update_missing_values(
    audit: dict,
    dataframe: pd.DataFrame
) -> None:
    """Record missing values found in raw data."""

    missing_counts = dataframe.isna().sum()

    for column, count in missing_counts.items():
        update_audit_counter(
            audit["raw"]["missing_values"],
            column,
            int(count)
        )


def save_audit_report(
    audit: dict,
    audit_path: str
) -> None:
    """Save the final audit report as JSON."""

    output_path = Path(audit_path)

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with output_path.open(
        mode="w",
        encoding="utf-8"
    ) as file:
        json.dump(
            audit,
            file,
            indent=4
        )


# --------------------------------------------------
# Type conversion
# --------------------------------------------------

def prepare_data_types(
    dataframe: pd.DataFrame
) -> pd.DataFrame:
    """Convert timestamp and numeric columns."""

    prepared = dataframe.copy()

    for column in DATETIME_COLUMNS:
        if column in prepared.columns:
            prepared[column] = pd.to_datetime(
                prepared[column],
                errors="coerce"
            )

    for column in NUMERIC_COLUMNS:
        if column in prepared.columns:
            prepared[column] = pd.to_numeric(
                prepared[column],
                errors="coerce"
            )

    return prepared


# --------------------------------------------------
# Validation
# --------------------------------------------------

def build_validation_masks(
    dataframe: pd.DataFrame,
    valid_location_ids: set
) -> dict[str, pd.Series]:
    """Build masks for conditions requiring rejection."""

    trip_duration = (
        dataframe["tpep_dropoff_datetime"]
        - dataframe["tpep_pickup_datetime"]
    )

    return {
        "invalid_timestamps": (
            dataframe["tpep_pickup_datetime"].isna()
            | dataframe["tpep_dropoff_datetime"].isna()
        ),

        "invalid_reporting_period": (
            dataframe["tpep_pickup_datetime"]
            < REPORTING_PERIOD_START
        ) | (
            dataframe["tpep_pickup_datetime"]
            >= REPORTING_PERIOD_END
        ),

        "invalid_trip_duration": (
            trip_duration < pd.Timedelta(0)
        ) | (
            trip_duration > MAXIMUM_TRIP_DURATION
        ),

        "invalid_distance": (
            dataframe["trip_distance"] < 0
        ),

        "invalid_fare": (
            dataframe["fare_amount"] < 0
        ),

        "invalid_total_amount": (
            dataframe["total_amount"] < 0
        ),

        "invalid_location_id": (
            ~dataframe["PULocationID"].isin(valid_location_ids)
            | ~dataframe["DOLocationID"].isin(valid_location_ids)
        )
    }

def inspect_raw_batch(
    dataframe: pd.DataFrame,
    valid_location_ids: set,
    audit: dict
) -> None:
    """Inspect raw data before corrections are applied."""

    audit["raw"]["rows"] += len(dataframe)

    update_missing_values(
        audit,
        dataframe
    )

    audit["raw"]["duplicate_rows"] += int(
        dataframe.duplicated().sum()
    )

    raw_masks = build_validation_masks(
        dataframe,
        valid_location_ids
    )

    for check_name, mask in raw_masks.items():
        audit["raw"][check_name] += int(
            mask.fillna(True).sum()
        )


# --------------------------------------------------
# Cleaning
# --------------------------------------------------

def convert_negative_to_positive(
    dataframe: pd.DataFrame,
    audit: dict
) -> pd.DataFrame:
    """Convert negative values only in configured amount columns."""

    corrected = dataframe.copy()

    for column in POSITIVE_VALUE_COLUMNS:
        if column not in corrected.columns:
            continue

        negative_mask = corrected[column].lt(0).fillna(False)
        corrected_count = int(negative_mask.sum())

        if corrected_count == 0:
            continue

        corrected.loc[negative_mask, column] = (
            corrected.loc[negative_mask, column].abs()
        )

        update_audit_counter(
            audit["processing"]["negative_values_corrected"],
            column,
            corrected_count
        )

    return corrected


def handle_missing_values(
    dataframe: pd.DataFrame,
    audit: dict
) -> pd.DataFrame:
    """Replace missing optional charge values with zero."""

    corrected = dataframe.copy()

    for column in OPTIONAL_CHARGE_COLUMNS:
        if column not in corrected.columns:
            continue

        missing_count = int(
            corrected[column].isna().sum()
        )

        if missing_count == 0:
            continue

        corrected[column] = corrected[column].fillna(0)

        update_audit_counter(
            audit["processing"]["missing_values_replaced"],
            column,
            missing_count
        )

    return corrected


def standardize_store_and_forward_flag(
    dataframe: pd.DataFrame,
    audit: dict
) -> pd.DataFrame:
    """Standardize store_and_fwd_flag to Y, N, or null."""

    corrected = dataframe.copy()

    column = "store_and_fwd_flag"

    if column not in corrected.columns:
        return corrected

    original_values = corrected[column].copy()

    corrected[column] = (
        corrected[column]
        .astype("string")
        .str.strip()
        .str.upper()
    )

    corrected[column] = corrected[column].replace({
        "YES": "Y",
        "NO": "N",
        "1": "Y",
        "0": "N"
    })

    invalid_mask = (
        corrected[column].notna()
        & ~corrected[column].isin(["Y", "N"])
    )

    invalid_count = int(invalid_mask.sum())

    corrected.loc[invalid_mask, column] = pd.NA

    changed_count = int(
        (
            original_values.astype("string")
            != corrected[column].astype("string")
        ).fillna(False).sum()
    )

    if changed_count > 0:
        update_audit_counter(
            audit["processing"]["category_values_replaced"],
            column,
            changed_count
        )

    if invalid_count > 0:
        update_audit_counter(
            audit["processing"]["category_values_replaced"],
            f"{column}_invalid_to_null",
            invalid_count
        )

    return corrected


def standardize_category_codes(
    dataframe: pd.DataFrame,
    audit: dict
) -> pd.DataFrame:
    """Replace unknown categorical codes with null."""

    corrected = dataframe.copy()

    for column, allowed_values in VALID_CATEGORY_VALUES.items():
        if column not in corrected.columns:
            continue

        invalid_mask = (
            corrected[column].notna()
            & ~corrected[column].isin(allowed_values)
        )

        invalid_count = int(invalid_mask.sum())

        if invalid_count == 0:
            continue

        corrected.loc[invalid_mask, column] = pd.NA

        update_audit_counter(
            audit["processing"]["category_values_replaced"],
            column,
            invalid_count
        )

    return corrected


def clean_passenger_count(
    dataframe: pd.DataFrame,
    audit: dict
) -> pd.DataFrame:
    """Replace unrealistic passenger counts with null."""

    corrected = dataframe.copy()

    if "passenger_count" not in corrected.columns:
        return corrected

    invalid_mask = (
        corrected["passenger_count"].lt(0)
        | corrected["passenger_count"].gt(8)
    ).fillna(False)

    invalid_count = int(invalid_mask.sum())

    corrected.loc[
        invalid_mask,
        "passenger_count"
    ] = pd.NA

    audit["processing"][
        "passenger_counts_replaced"
    ] += invalid_count

    return corrected


def add_record_id(
    dataframe: pd.DataFrame
) -> pd.DataFrame:
    """Create a hash used to detect duplicate rows."""

    dataframe_with_id = dataframe.copy()

    record_hashes = pd.util.hash_pandas_object(
        dataframe_with_id,
        index=False
    )

    dataframe_with_id.insert(
        0,
        "_record_id",
        record_hashes.map(
            lambda value: f"{value:016x}"
        )
    )

    return dataframe_with_id


def separate_rejected_rows(
    dataframe: pd.DataFrame,
    valid_location_ids: set
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Separate valid rows from rows that remain invalid."""

    corrected_masks = build_validation_masks(
        dataframe,
        valid_location_ids
    )

    reject_mask = pd.Series(
        False,
        index=dataframe.index,
        dtype=bool
    )

    rejection_reasons = pd.Series(
        "",
        index=dataframe.index,
        dtype="string"
    )

    for check_name, mask in corrected_masks.items():
        active_mask = mask.fillna(True)

        reject_mask |= active_mask

        rejection_reasons.loc[active_mask] += (
            check_name + ";"
        )

    rejected = dataframe.loc[reject_mask].copy()

    rejected["_rejection_reason"] = (
        rejection_reasons.loc[reject_mask]
        .str.rstrip(";")
    )

    valid = dataframe.loc[~reject_mask].copy()

    return valid, rejected


def clean_batch(
    dataframe: pd.DataFrame,
    valid_location_ids: set,
    audit: dict
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Run all cleaning and standardization steps."""

    corrected = convert_negative_to_positive(
        dataframe,
        audit
    )

    corrected = handle_missing_values(
        corrected,
        audit
    )

    corrected = standardize_store_and_forward_flag(
        corrected,
        audit
    )

    corrected = standardize_category_codes(
        corrected,
        audit
    )

    corrected = clean_passenger_count(
        corrected,
        audit
    )

    valid, rejected = separate_rejected_rows(
        corrected,
        valid_location_ids
    )

    valid = add_record_id(valid)

    return valid, rejected


# --------------------------------------------------
# Database creation and insertion
# --------------------------------------------------

def create_valid_database(
    connection: sqlite3.Connection,
    zones_dataframe: pd.DataFrame
) -> None:
    """Reset the valid database and store taxi zones."""

    connection.execute(
        "DROP TABLE IF EXISTS taxi_trips"
    )

    connection.execute(
        "DROP TABLE IF EXISTS taxi_zones"
    )

    zones_dataframe.to_sql(
        name="taxi_zones",
        con=connection,
        if_exists="replace",
        index=False
    )

    connection.commit()


def initialize_trip_table(
    connection: sqlite3.Connection,
    dataframe: pd.DataFrame
) -> None:
    """Create the taxi-trips table and duplicate-control index."""

    dataframe.head(0).to_sql(
        name="taxi_trips",
        con=connection,
        if_exists="replace",
        index=False
    )

    connection.execute(
        """
        CREATE UNIQUE INDEX idx_taxi_trips_record_id
        ON taxi_trips (_record_id)
        """
    )

    connection.commit()


def insert_valid_batch(
    connection: sqlite3.Connection,
    dataframe: pd.DataFrame
) -> int:
    """Insert valid rows and ignore duplicate record IDs."""

    if dataframe.empty:
        return 0

    before_count = connection.execute(
        "SELECT COUNT(*) FROM taxi_trips"
    ).fetchone()[0]

    dataframe.to_sql(
        name="taxi_trips_staging",
        con=connection,
        if_exists="replace",
        index=False
    )

    quoted_columns = [
        f'"{column}"'
        for column in dataframe.columns
    ]

    column_list = ", ".join(quoted_columns)

    connection.execute(
        f"""
        INSERT OR IGNORE INTO taxi_trips ({column_list})
        SELECT {column_list}
        FROM taxi_trips_staging
        """
    )

    connection.execute(
        "DROP TABLE taxi_trips_staging"
    )

    connection.commit()

    after_count = connection.execute(
        "SELECT COUNT(*) FROM taxi_trips"
    ).fetchone()[0]

    return after_count - before_count


def store_rejected_rows(
    connection: sqlite3.Connection,
    dataframe: pd.DataFrame
) -> None:
    """Store rows that remain invalid after cleaning."""

    if dataframe.empty:
        return

    dataframe.to_sql(
        name="rejected_taxi_trips",
        con=connection,
        if_exists="append",
        index=False
    )

    connection.commit()


# --------------------------------------------------
# Final database validation
# --------------------------------------------------

def validate_database(
    connection: sqlite3.Connection,
    audit: dict
) -> None:
    """Validate the finished valid-data database."""

    result = connection.execute(
        """
        SELECT
            COUNT(*) AS total_rows,

            COUNT(*) - COUNT(DISTINCT _record_id)
                AS duplicate_rows,

            SUM(
                CASE WHEN trip_distance < 0
                THEN 1 ELSE 0 END
            ) AS invalid_distance,

            SUM(
                CASE WHEN fare_amount < 0
                THEN 1 ELSE 0 END
            ) AS invalid_fare,

            SUM(
                CASE WHEN total_amount < 0
                THEN 1 ELSE 0 END
            ) AS invalid_total_amount,

            SUM(
                CASE
                    WHEN tpep_dropoff_datetime
                         < tpep_pickup_datetime
                    THEN 1 ELSE 0
                END
            ) AS invalid_trip_duration,

            SUM(
                CASE
                    WHEN PULocationID IS NULL
                         OR DOLocationID IS NULL
                    THEN 1 ELSE 0
                END
            ) AS missing_location_id

        FROM taxi_trips
        """
    ).fetchone()

    audit["database"] = {
        "rows": int(result[0]),
        "duplicate_rows": int(result[1]),
        "invalid_distance": int(result[2] or 0),
        "invalid_fare": int(result[3] or 0),
        "invalid_total_amount": int(result[4] or 0),
        "invalid_trip_duration": int(result[5] or 0),
        "missing_location_id": int(result[6] or 0)
    }

    expected_rows = (
        audit["raw"]["rows"]
        - audit["processing"]["rejected_rows"]
        - audit["processing"]["duplicate_rows_removed"]
    )

    audit["validation"] = {
        "expected_database_rows": expected_rows,

        "row_count_matches": (
            expected_rows == audit["database"]["rows"]
        ),

        "duplicates_cleared": (
            audit["database"]["duplicate_rows"] == 0
        ),

        "invalid_values_cleared": all([
            audit["database"]["invalid_distance"] == 0,
            audit["database"]["invalid_fare"] == 0,
            audit["database"]["invalid_total_amount"] == 0,
            audit["database"]["invalid_trip_duration"] == 0,
            audit["database"]["missing_location_id"] == 0
        ])
    }


# --------------------------------------------------
# Complete pipeline
# --------------------------------------------------

def run_pipeline() -> None:
    """Run the complete batch ETL process."""

    Path(DATABASE_PATH).parent.mkdir(
        parents=True,
        exist_ok=True
    )

    audit = create_audit_report()

    zones_dataframe = pd.read_csv(
        ZONES_PATH
    )

    valid_location_ids = set(
        zones_dataframe["LocationID"].dropna()
    )

    parquet_file = pq.ParquetFile(
        TRIPS_PATH
    )

    with (
        sqlite3.connect(DATABASE_PATH) as valid_connection,
        sqlite3.connect(INVALID_DATABASE_PATH) as invalid_connection
    ):
        create_valid_database(
            valid_connection,
            zones_dataframe
        )

        invalid_connection.execute(
            "DROP TABLE IF EXISTS rejected_taxi_trips"
        )

        invalid_connection.commit()

        table_initialized = False

        for batch_number, arrow_batch in enumerate(
            parquet_file.iter_batches(
                batch_size=BATCH_SIZE
            ),
            start=1
        ):
            raw_batch = arrow_batch.to_pandas()

            prepared_batch = prepare_data_types(
                raw_batch
            )

            # Audit the original data before corrections.
            inspect_raw_batch(
                dataframe=prepared_batch,
                valid_location_ids=valid_location_ids,
                audit=audit
            )

            # Correct, standardize and separate the data.
            cleaned_batch, rejected_batch = clean_batch(
                dataframe=prepared_batch,
                valid_location_ids=valid_location_ids,
                audit=audit
            )

            rejected_rows = len(rejected_batch)

            audit["processing"]["rejected_rows"] += (
                rejected_rows
            )

            store_rejected_rows(
                invalid_connection,
                rejected_batch
            )

            if not table_initialized:
                initialize_trip_table(
                    valid_connection,
                    cleaned_batch
                )

                table_initialized = True

            inserted_rows = insert_valid_batch(
                valid_connection,
                cleaned_batch
            )

            duplicate_rows = (
                len(cleaned_batch) - inserted_rows
            )

            audit["processing"][
                "duplicate_rows_removed"
            ] += duplicate_rows

            audit["processing"][
                "rows_sent_to_database"
            ] += inserted_rows

            print(
                f"Batch {batch_number}: "
                f"{len(raw_batch):,} raw, "
                f"{rejected_rows:,} rejected, "
                f"{duplicate_rows:,} duplicates, "
                f"{inserted_rows:,} inserted"
            )

        validate_database(
            valid_connection,
            audit
        )

    save_audit_report(
        audit,
        AUDIT_PATH
    )

    print("\nPipeline completed.")
    print(f"Valid database: {DATABASE_PATH}")
    print(f"Rejected database: {INVALID_DATABASE_PATH}")
    print(f"Audit report: {AUDIT_PATH}")


if __name__ == "__main__":
    run_pipeline()