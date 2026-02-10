import os
import uuid
import tempfile
import logging
from datetime import date

import pandas as pd
from django.core.management.base import BaseCommand
from google.cloud import bigquery

from reports.sync import _parse_sources, _fetch_maria_rows, _fetch_reseller_map, log_sync_event

logger = logging.getLogger(__name__)


def _get_bq_config():
    project = os.getenv('BQ_PROJECT')
    dataset = os.getenv('BQ_DATASET')
    table = os.getenv('BQ_TABLE')
    location = os.getenv('BQ_LOCATION', 'US')
    if not (project and dataset and table):
        raise RuntimeError('BigQuery config missing: BQ_PROJECT, BQ_DATASET, BQ_TABLE')
    return project, dataset, table, location


def _load_df_to_bq(df, table_id, client, location, write_disposition):
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        autodetect=True,
        source_format=bigquery.SourceFormat.CSV,
    )
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(mode='w+', suffix='.csv', delete=False, encoding='utf-8') as tmp:
            df.to_csv(tmp.name, index=False)
            tmp_path = tmp.name
        with open(tmp_path, 'rb') as handle:
            load_job = client.load_table_from_file(
                handle,
                table_id,
                job_config=job_config,
                location=location,
            )
            load_job.result()
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)


class Command(BaseCommand):
    help = 'Backfill report_user_service from hspdata (< cutoff) plus MariaDB (>= cutoff).'

    def add_arguments(self, parser):
        parser.add_argument('--cutoff-date', type=str, default='2025-10-31',
                            help='Cutoff date (YYYY-MM-DD). hspdata is used before this date.')
        parser.add_argument('--hsp-table', type=str, default='',
                            help='Full BigQuery table ID for hspdata (project.dataset.table).')
        parser.add_argument('--target-table', type=str, default='',
                            help='Full BigQuery target table ID (project.dataset.table).')

    def handle(self, *args, **options):
        project, dataset, default_table, location = _get_bq_config()
        cutoff_date = options['cutoff_date']
        try:
            date.fromisoformat(cutoff_date)
        except ValueError as exc:
            raise RuntimeError(f'Invalid cutoff date: {cutoff_date}') from exc

        target_table = options['target_table'].strip() or f"{project}.{dataset}.{default_table}"
        hsp_table = options['hsp_table'].strip() or os.getenv('BQ_HSP_TABLE', '').strip()
        if not hsp_table:
            hsp_table = f"{project}.{dataset}.hspdata"

        sources = _parse_sources()
        if not sources:
            raise RuntimeError('No MariaDB sources configured.')

        logger.info('Backfill: loading MariaDB rows >= %s', cutoff_date)
        log_sync_event('backfill_start', 'Starting report_user_service backfill',
                       target_table=target_table, hsp_table=hsp_table, cutoff_date=cutoff_date)

        client = bigquery.Client(project=project)
        stage_suffix = uuid.uuid4().hex
        maria_stage = f"{project}.{dataset}.report_user_service_maria_stage_{stage_suffix}"
        map_stage = f"{project}.{dataset}.report_user_service_reseller_map_{stage_suffix}"

        try:
            maria_loaded = False
            all_map = []
            for source in sources:
                df = _fetch_maria_rows(source, start_date=cutoff_date)
                if not df.empty:
                    write_disp = 'WRITE_TRUNCATE' if not maria_loaded else 'WRITE_APPEND'
                    _load_df_to_bq(df, maria_stage, client, location, write_disp)
                    maria_loaded = True
                map_df = _fetch_reseller_map(source)
                if not map_df.empty:
                    all_map.append(map_df)

            if not maria_loaded:
                raise RuntimeError('No MariaDB rows returned for cutoff date.')

            map_df = pd.concat(all_map, ignore_index=True) if all_map else pd.DataFrame()
            if not map_df.empty:
                map_df = map_df[map_df['creator_norm'].notna()]
                map_df = map_df[map_df['creator_norm'].astype(str).str.strip() != '']
                map_df = map_df.drop_duplicates(subset=['creator_norm'], keep='first')
                _load_df_to_bq(map_df, map_stage, client, location, 'WRITE_TRUNCATE')
            else:
                schema = [
                    bigquery.SchemaField('creator_norm', 'STRING'),
                    bigquery.SchemaField('rs_userid', 'INTEGER'),
                    bigquery.SchemaField('rs_name', 'STRING'),
                ]
                table = bigquery.Table(map_stage, schema=schema)
                client.create_table(table)

            query = f"""
CREATE OR REPLACE TABLE `{target_table}` AS
WITH maria AS (
  SELECT
    SAFE_CAST(CreateDate AS DATE) AS CreateDate,
    SAFE_CAST(rs_userid AS INT64) AS rs_userid,
    rs_username,
    rs_name,
    SAFE_CAST(UserServiceID AS INT64) AS UserServiceID,
    username,
    ServiceName,
    SAFE_CAST(ServicePrice AS FLOAT64) AS ServicePrice,
    SAFE_CAST(Package AS FLOAT64) AS Package,
    ServiceStatus,
    SAFE_CAST(StartDate AS DATE) AS StartDate,
    SAFE_CAST(EndDate AS DATE) AS EndDate
  FROM `{maria_stage}`
  WHERE SAFE_CAST(CreateDate AS DATE) >= DATE(@cutoff_date)
),
base_hsp AS (
  SELECT
    SAFE_CAST(h.CreatDate AS DATE) AS CreateDate,
    m.rs_userid AS rs_userid,
    h.Creator AS rs_username,
    m.rs_name AS rs_name,
    SAFE_CAST(h.UserServiceId AS INT64) AS UserServiceID,
    h.Username AS username,
    h.ServiceName AS ServiceName,
    SAFE_CAST(h.ServicePrice AS FLOAT64) AS ServicePrice,
    SAFE_CAST(h.Package AS FLOAT64) AS Package,
    h.ServiceStatus AS ServiceStatus,
    SAFE_CAST(h.StartDate AS DATE) AS StartDate,
    SAFE_CAST(h.EndDate AS DATE) AS EndDate
  FROM `{hsp_table}` h
  LEFT JOIN `{map_stage}` m
    ON LOWER(TRIM(h.Creator)) = m.creator_norm
  LEFT JOIN `{maria_stage}` ms
    ON SAFE_CAST(h.UserServiceId AS INT64) = SAFE_CAST(ms.UserServiceID AS INT64)
  WHERE SAFE_CAST(h.CreatDate AS DATE) < DATE(@cutoff_date)
    AND ms.UserServiceID IS NULL
)
SELECT
  ROW_NUMBER() OVER (ORDER BY rs_username, UserServiceID, CreateDate DESC) AS id,
  CreateDate,
  rs_userid,
  rs_username,
  rs_name,
  UserServiceID,
  username,
  ServiceName,
  ServicePrice,
  Package,
  ServiceStatus,
  StartDate,
  EndDate
FROM (
  SELECT * FROM maria
  UNION ALL
  SELECT * FROM base_hsp
)
"""
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter('cutoff_date', 'DATE', cutoff_date),
                ]
            )
            query_job = client.query(query, job_config=job_config, location=location)
            query_job.result()
            log_sync_event('backfill_success', 'Backfill completed', target_table=target_table)
            self.stdout.write(self.style.SUCCESS('Backfill completed.'))
        finally:
            client.delete_table(maria_stage, not_found_ok=True)
            client.delete_table(map_stage, not_found_ok=True)
