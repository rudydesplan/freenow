from airflow.sdk import dag, task
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from pendulum import datetime
import logging
import uuid
import hashlib
import boto3

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from astro import sql as aql
from astro.files import File
from astro.constants import FileType
from astro.sql.table import Table, Metadata
import sqlalchemy as sa


# ----------------------------
# Connections
# ----------------------------
AWS_CONN_ID = "aws_default"
POSTGRES_CONN_ID = "my_postgres_conn"


# ----------------------------
# Dataset Config
# ----------------------------
DATASETS = [
    {"name": "drivers",  "s3_var": "S3_DRIVERS_URI"},
    {"name": "offers",   "s3_var": "S3_OFFERS_URI"},
    {"name": "bookings", "s3_var": "S3_BOOKINGS_URI"},
]

TABLE_SPECS = {
    "drivers":  ["id", "Date_registration", "Driver_rating", "Rating_count", "Receive_marketing", "Country"],
    "offers":   ["id", "Datecreated", "bookingid", "driverid", "routedistance", "state", "driverread"],
    "bookings": ["Id", "request_date", "status", "Id_driver", "Estimated_route_fare"],
}

BRONZE_COLS = {
    "drivers": "id, date_registration, driver_rating, rating_count, receive_marketing, country",
    "offers": "id, datecreated, bookingid, driverid, routedistance, state, driverread",
    "bookings": "id, request_date, status, id_driver, estimated_route_fare",
}


@dag(
    start_date=datetime(2026, 2, 1),
    schedule=None,
    catchup=False,
    tags=["bronze", "postgres", "dynamic-mapping", "production"],
)
def freenow_s3_to_bronze():

    # -------------------------------------------------
    # 1️⃣ Generate Batch ID
    # -------------------------------------------------
    @task
    def generate_batch_id():
        logger = logging.getLogger(__name__)
        batch_id = str(uuid.uuid4())
        logger.info(f"Generated new batch_id: {batch_id}")
        return batch_id

    batch_id = generate_batch_id()

    # -------------------------------------------------
    # 2️⃣ Enrich dataset with S3 path
    # -------------------------------------------------
    @task
    def enrich_dataset(dataset: dict):
        logger = logging.getLogger(__name__)

        dataset["s3_path"] = Variable.get(dataset["s3_var"])

        logger.info(
            f"[{dataset['name']}] Retrieved S3 path from Variable "
            f"{dataset['s3_var']} -> {dataset['s3_path']}"
        )

        return dataset

    enriched = enrich_dataset.expand(dataset=DATASETS)



    # -------------------------------------------------
    # 3️⃣ Compute true MD5
    # -------------------------------------------------
    @task
    def compute_md5(dataset: dict):
        logger = logging.getLogger(__name__)

        logger.info(f"[{dataset['name']}] Starting MD5 computation")
        logger.info(f"[{dataset['name']}] S3 path: {dataset['s3_path']}")

        bucket, key = S3Hook().parse_s3_str(dataset["s3_path"])
        s3_client = boto3.client("s3")

        response = s3_client.get_object(Bucket=bucket, Key=key)

        md5_hash = hashlib.md5()
        total_bytes = 0

        for chunk in iter(lambda: response["Body"].read(8 * 1024 * 1024), b""):
            total_bytes += len(chunk)
            md5_hash.update(chunk)

        dataset["file_md5"] = md5_hash.hexdigest()

        logger.info(f"[{dataset['name']}] Finished MD5 computation")
        logger.info(f"[{dataset['name']}] Total bytes processed: {total_bytes}")
        logger.info(f"[{dataset['name']}] MD5: {dataset['file_md5']}")

        return dataset

    with_md5 = compute_md5.expand(dataset=enriched)


    # -------------------------------------------------
    # 4️⃣ Idempotency Check (Postgres)
    # -------------------------------------------------
    @task
    def check_idempotency(dataset: dict):
        logger = logging.getLogger(__name__)

        logger.info(f"[{dataset['name']}] Checking idempotency")
        logger.info(f"[{dataset['name']}] MD5: {dataset['file_md5']}")

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        sql = """
            SELECT 1
            FROM bronze.ingestion_files
            WHERE dataset_name = %s
              AND file_md5 = %s
              AND load_status = 'SUCCESS';
        """

        result = hook.get_first(
            sql,
            parameters=(dataset["name"], dataset["file_md5"]),
        )

        if result:
            logger.warning(
                f"[{dataset['name']}] Already ingested with same MD5. Skipping."
            )
            raise AirflowSkipException(
                f"{dataset['name']} already ingested with same MD5"
            )

        logger.info(f"[{dataset['name']}] No previous successful ingestion found.")
        return dataset

    filtered = check_idempotency.expand(dataset=with_md5)


    # -------------------------------------------------
    # 5️⃣ Register RUNNING
    # -------------------------------------------------
    start_audit = aql.run_raw_sql.partial(
        conn_id=POSTGRES_CONN_ID,
    ).expand(
        sql=[
            """
            INSERT INTO bronze.ingestion_files (
                batch_id,
                dataset_name,
                source_uri,
                file_format,
                file_md5,
                load_started_at,
                load_status
            )
            VALUES (
                CAST('{{ ti.xcom_pull(task_ids="generate_batch_id") }}' AS UUID),
                %(name)s,
                %(s3_path)s,
                'csv',
                %(file_md5)s,
                CURRENT_TIMESTAMP,
                'RUNNING'
            );
            """,
        ],
        parameters=filtered,
    )

    # -------------------------------------------------
    # 6️⃣ Load to Postgres staging
    # -------------------------------------------------
    def stage_table(dataset):
        return Table(
            name=f"stage_{dataset['name']}_csv",
            conn_id=POSTGRES_CONN_ID,
            metadata=Metadata(schema="bronze"),
            columns=[sa.Column(c, sa.String) for c in TABLE_SPECS[dataset["name"]]],
        )

    load_stage = aql.load_file.partial(
        conn_id=POSTGRES_CONN_ID,
        if_exists="replace",
        assume_schema_exists=True,
        use_native_support=False,
    ).expand(
        input_file=filtered.map(
            lambda d: File(
                path=d["s3_path"],
                conn_id=AWS_CONN_ID,
                filetype=FileType.CSV,
            )
        ),
        output_table=filtered.map(stage_table),
    )

    # -------------------------------------------------
    # 7️⃣ Insert into Bronze
    # -------------------------------------------------
    insert_bronze = aql.run_raw_sql.partial(
        conn_id=POSTGRES_CONN_ID,
    ).expand(
        sql=filtered.map(
            lambda d: f"""
            DELETE FROM bronze.raw_{d['name']}
            WHERE _file_md5 = '{d['file_md5']}';

            INSERT INTO bronze.raw_{d['name']}
            ({BRONZE_COLS[d['name']]}, _batch_id, _source_uri, _file_md5, _ingested_at, _row_number)
            SELECT *,
                   CAST('{{{{ ti.xcom_pull(task_ids="generate_batch_id") }}}}' AS UUID),
                   '{d["s3_path"]}',
                   '{d["file_md5"]}',
                   CURRENT_TIMESTAMP,
                   ROW_NUMBER() OVER ()
            FROM bronze.stage_{d['name']}_csv;

            DROP TABLE IF EXISTS bronze.stage_{d['name']}_csv;
            """
        )
    )

    # -------------------------------------------------
    # 8️⃣ Mark SUCCESS
    # -------------------------------------------------
    mark_success = aql.run_raw_sql.partial(
        conn_id=POSTGRES_CONN_ID,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    ).expand(
        sql=filtered.map(
            lambda d: f"""
            UPDATE bronze.ingestion_files
            SET load_status = 'SUCCESS',
                rows_loaded = (
                    SELECT COUNT(*)
                    FROM bronze.raw_{d['name']}
                    WHERE _file_md5 = '{d['file_md5']}'
                ),
                load_finished_at = CURRENT_TIMESTAMP
            WHERE dataset_name = '{d['name']}'
              AND batch_id = CAST('{{{{ ti.xcom_pull(task_ids="generate_batch_id") }}}}' AS UUID);
            """
        )
    )

    # -------------------------------------------------
    # 9️⃣ Mark FAILED
    # -------------------------------------------------
    mark_failed = aql.run_raw_sql.partial(
        conn_id=POSTGRES_CONN_ID,
        trigger_rule=TriggerRule.ONE_FAILED,
    ).expand(
        sql=filtered.map(
            lambda d: f"""
            UPDATE bronze.ingestion_files
            SET load_status = 'FAILED',
                error_message = 'Bronze ingestion failed',
                load_finished_at = CURRENT_TIMESTAMP
            WHERE dataset_name = '{d['name']}'
              AND batch_id = CAST('{{{{ ti.xcom_pull(task_ids="generate_batch_id") }}}}' AS UUID);
            """
        )
    )

    # -------------------------------------------------
    # 🔗 Proper Dependencies (Mapped-aware)
    # -------------------------------------------------

    batch_id >> enriched >> with_md5 >> filtered

    filtered >> start_audit
    start_audit >> load_stage
    load_stage >> insert_bronze

    insert_bronze >> mark_success
    [start_audit, load_stage, insert_bronze] >> mark_failed


dag = freenow_s3_to_bronze()