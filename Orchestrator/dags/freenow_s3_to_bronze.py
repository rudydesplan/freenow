import os
from airflow.sdk import dag, task
from airflow.task.trigger_rule import TriggerRule
from airflow.exceptions import AirflowSkipException
from pendulum import datetime
import logging
import uuid
import hashlib

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


# ----------------------------
# Connections
# ----------------------------
AWS_CONN_ID = "aws_default"          # (géré via conn Airflow)
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
  "drivers":  ["id", "country", "rating", "rating_count", "date_registration", "receive_marketing"],
  "offers":   ["id", "datecreated", "bookingid", "driverid", "routedistance", "state", "driverread"],
  "bookings": ["id", "request_date", "status", "id_driver", "estimated_route_fare"],
}

BRONZE_COLS = {
    "drivers": "id, country, rating, rating_count, date_registration, receive_marketing",
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
        logger.info("Generated new batch_id: %s", batch_id)
        return batch_id

    batch_id = generate_batch_id()

    # -------------------------------------------------
    # 2️⃣ Enrich dataset with S3 path
    # -------------------------------------------------
    @task
    def enrich_dataset(dataset: dict):
        logger = logging.getLogger(__name__)

        key = dataset["s3_var"]

        if key not in os.environ:
            raise ValueError(f"Environment variable {key} not found")

        dataset["s3_path"] = os.environ[key]

        logger.info(
            "[%s] S3 path resolved from ENV %s -> %s",
            dataset["name"],
            key,
            dataset["s3_path"],
        )

        return dataset

    enriched = enrich_dataset.expand(dataset=DATASETS)

    # -------------------------------------------------
    # 3️⃣ Compute true MD5
    # -------------------------------------------------
    @task
    def compute_md5(dataset: dict):
        logger = logging.getLogger(__name__)

        name = dataset["name"]
        s3_path = dataset["s3_path"]

        logger.info("[%s] Starting MD5 computation", name)
        logger.info("[%s] S3 path: %s", name, s3_path)

        hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        bucket, key = hook.parse_s3_url(s3_path)

        logger.info("[%s] Resolved bucket=%s key=%s", name, bucket, key)

        obj = hook.get_key(key=key, bucket_name=bucket)

        md5_hash = hashlib.md5()
        total_bytes = 0
        chunk_size = 8 * 1024 * 1024  # 8 MB

        logger.info("[%s] Streaming S3 object for MD5 computation...", name)

        body = obj.get()["Body"]

        try:
            for chunk in iter(lambda: body.read(chunk_size), b""):
                chunk_len = len(chunk)
                total_bytes += chunk_len
                md5_hash.update(chunk)

            dataset["file_md5"] = md5_hash.hexdigest()

        finally:
            body.close()

        logger.info("[%s] Finished MD5 computation", name)
        logger.info("[%s] Total bytes processed: %s", name, total_bytes)
        logger.info("[%s] MD5: %s", name, dataset["file_md5"])

        return dataset

    with_md5 = compute_md5.expand(dataset=enriched)

    # -------------------------------------------------
    # 4️⃣ Idempotency Check (Postgres)
    # -------------------------------------------------
    @task
    def check_idempotency(dataset: dict):
        logger = logging.getLogger(__name__)
        logger.info("[%s] Checking idempotency (MD5=%s)", dataset["name"], dataset["file_md5"])

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql = """
            SELECT 1
            FROM bronze.ingestion_files
            WHERE dataset_name = %s
              AND file_md5 = %s
              AND load_status = 'SUCCESS';
        """
        result = hook.get_first(sql, parameters=(dataset["name"], dataset["file_md5"]))

        if result:
            logger.warning("[%s] Already ingested with same MD5. Skipping.", dataset["name"])
            raise AirflowSkipException(f"{dataset['name']} already ingested with same MD5")

        logger.info("[%s] No previous successful ingestion found.", dataset["name"])
        return dataset

    filtered = check_idempotency.expand(dataset=with_md5)

    # -------------------------------------------------
    # ✅ Attach batch_id to each mapped dataset (NEW)
    # -------------------------------------------------
    @task
    def attach_batch_id(dataset: dict, batch_id: str):
        dataset["batch_id"] = batch_id
        return dataset

    with_batch = attach_batch_id.partial(batch_id=batch_id).expand(dataset=filtered)

    # -------------------------------------------------
    # 5️⃣ Register RUNNING  (SQLExecuteQueryOperator)
    # -------------------------------------------------
    start_audit = SQLExecuteQueryOperator.partial(
        task_id="start_audit",
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
                CAST(%(batch_id)s AS UUID),
                %(name)s,
                %(s3_path)s,
                'csv',
                %(file_md5)s,
                CURRENT_TIMESTAMP,
                'RUNNING'
            );
            """
        ],
        parameters=with_batch,
    )

    # -------------------------------------------------
    # 6️⃣ Load to Postgres staging (S3 -> local -> PostgresHook.copy_expert)
    # -------------------------------------------------
    @task
    def load_stage(dataset: dict):
        """
        - Télécharge le CSV depuis S3 vers /tmp/<dataset>/
        - (Re)crée bronze.stage_<dataset>_csv en TEXT
        - COPY ... FROM STDIN via PostgresHook.copy_expert
        """
        logger = logging.getLogger(__name__)
        name = dataset["name"]
        s3_path = dataset["s3_path"]

        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        bucket, key = s3_hook.parse_s3_url(s3_path)

        local_dir = f"/tmp/{name}"
        os.makedirs(local_dir, exist_ok=True)

        logger.info("[%s] Downloading %s to directory %s", name, s3_path, local_dir)

        local_file = s3_hook.download_file(
            key=key,
            bucket_name=bucket,
            local_path=local_dir,
            preserve_file_name=True,
            use_autogenerated_subdir=False,
        )

        logger.info("[%s] File downloaded successfully to %s", name, local_file)

        cols = TABLE_SPECS[name]
        columns_sql = ", ".join(f'"{c}" TEXT' for c in cols)

        ddl = f"""
        DROP TABLE IF EXISTS bronze.stage_{name}_csv;
        CREATE TABLE bronze.stage_{name}_csv (
            {columns_sql}
        );
        """
        logger.info("[%s] Creating stage table bronze.stage_%s_csv", name, name)
        pg_hook.run(ddl)

        copy_sql = f"""
            COPY bronze.stage_{name}_csv
            FROM STDIN
            WITH (FORMAT csv, HEADER true)
        """
        logger.info("[%s] COPY into bronze.stage_%s_csv from %s", name, name, local_file)
        pg_hook.copy_expert(sql=copy_sql, filename=local_file)

        try:
            os.remove(local_file)
        except OSError:
            logger.warning("[%s] Could not delete temp file: %s", name, local_file)

        return dataset

    load_stage_task = load_stage.expand(dataset=with_batch)

    # -------------------------------------------------
    # 7️⃣ Insert into Bronze (SQLExecuteQueryOperator)
    # -------------------------------------------------
    insert_payload = with_batch.map(
        lambda d: {
            "sql": f"""
                DELETE FROM bronze.raw_{d['name']}
                WHERE _file_md5 = '{d['file_md5']}';

                INSERT INTO bronze.raw_{d['name']}
                ({BRONZE_COLS[d['name']]}, _batch_id, _source_uri, _file_md5, _ingested_at, _row_number)
                SELECT *,
                    CAST(%(batch_id)s AS UUID),
                    %(s3_path)s,
                    %(file_md5)s,
                    CURRENT_TIMESTAMP,
                    ROW_NUMBER() OVER ()
                FROM bronze.stage_{d['name']}_csv;

                TRUNCATE bronze.stage_{d['name']}_csv;
            """,
            "parameters": {
                "batch_id": d["batch_id"],
                "s3_path": d["s3_path"],
                "file_md5": d["file_md5"],
            },
        }
    )

    insert_bronze = SQLExecuteQueryOperator.partial(
        task_id="insert_bronze",
        conn_id=POSTGRES_CONN_ID,
    ).expand_kwargs(insert_payload)

    # -------------------------------------------------
    # 8️⃣ Mark SUCCESS (SQLExecuteQueryOperator)
    # -------------------------------------------------
    mark_success = SQLExecuteQueryOperator.partial(
        task_id="mark_success",
        conn_id=POSTGRES_CONN_ID,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    ).expand(
        sql=with_batch.map(
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
              AND batch_id = CAST(%(batch_id)s AS UUID);
            """
        ),
        parameters=with_batch,
    )

    # -------------------------------------------------
    # 9️⃣ Mark FAILED (SQLExecuteQueryOperator)
    # -------------------------------------------------
    mark_failed = SQLExecuteQueryOperator.partial(
        task_id="mark_failed",
        conn_id=POSTGRES_CONN_ID,
        trigger_rule=TriggerRule.ONE_FAILED,
    ).expand(
        sql=with_batch.map(
            lambda d: f"""
            UPDATE bronze.ingestion_files
            SET load_status = 'FAILED',
                error_message = 'Bronze ingestion failed',
                load_finished_at = CURRENT_TIMESTAMP
            WHERE dataset_name = '{d['name']}'
              AND batch_id = CAST(%(batch_id)s AS UUID);
            """
        ),
        parameters=with_batch,
    )

    # -------------------------------------------------
    # 🔗 Proper Dependencies (Mapped-aware)
    # -------------------------------------------------
    batch_id >> enriched >> with_md5 >> filtered >> with_batch

    with_batch >> start_audit
    start_audit >> load_stage_task
    load_stage_task >> insert_bronze

    insert_bronze >> mark_success
    [start_audit, load_stage_task, insert_bronze] >> mark_failed


dag = freenow_s3_to_bronze()
