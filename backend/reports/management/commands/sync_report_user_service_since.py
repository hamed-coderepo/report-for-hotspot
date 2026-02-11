import os
import time
import uuid
import tempfile
import logging
from datetime import date

import pandas as pd
from django.core.management.base import BaseCommand
from google.cloud import bigquery

from reports.sync import _parse_sources, _fetch_maria_rows, log_sync_event

logger = logging.getLogger(__name__)


def _get_bq_config():
    project = os.getenv('BQ_PROJECT')
    dataset = os.getenv('BQ_DATASET')
    table = os.getenv('BQ_TABLE')
    location = os.getenv('BQ_LOCATION', 'US')
    if not (project and dataset and table):
        raise RuntimeError('BigQuery config missing: BQ_PROJECT, BQ_DATASET, BQ_TABLE')
    return project, dataset, table, location


def _load_df_to_bq(df, table_id, client, location):
    job_config = bigquery.LoadJobConfig(
        write_disposition='WRITE_TRUNCATE',
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
    help = (
        'Sync report_user_service from MariaDB since a cutoff date (inclusive), '
        'replacing only that window in BigQuery.'
    )

    def add_arguments(self, parser):
        parser.add_argument('--start-date', type=str, default='2025-10-31',
                            help='Start date (YYYY-MM-DD), inclusive.')
        parser.add_argument('--end-date', type=str, default='',
                            help='Optional end date (YYYY-MM-DD), inclusive.')
        parser.add_argument('--limit', type=int, default=0, help='Optional row limit per source (0 = no limit).')
        parser.add_argument('--target-table', type=str, default='',
                            help='Full BigQuery target table ID (project.dataset.table).')

    def handle(self, *args, **options):
        started_at = time.time()
        project, dataset, default_table, location = _get_bq_config()
        start_date = options['start_date']
        end_date = options['end_date'].strip() or None
        limit = options['limit']

        self.stdout.write('Window sync: starting')
        self.stdout.write(f'Window sync: project={project} dataset={dataset} location={location}')
        self.stdout.write(f'Window sync: start_date={start_date} end_date={end_date or "(none)"} limit={limit}')

        try:
            date.fromisoformat(start_date)
        except ValueError as exc:
            raise RuntimeError(f'Invalid start date: {start_date}') from exc

        if end_date:
            try:
                date.fromisoformat(end_date)
            except ValueError as exc:
                raise RuntimeError(f'Invalid end date: {end_date}') from exc

        target_table = options['target_table'].strip() or f"{project}.{dataset}.{default_table}"
        self.stdout.write(f'Window sync: target_table={target_table}')

        sources = _parse_sources()
        if not sources:
            raise RuntimeError('No MariaDB sources configured.')

        self.stdout.write(f'Window sync: sources={len(sources)}')
        for source in sources:
            name = source.get('name') or source.get('host')
            host = source.get('host')
            db = source.get('db')
            self.stdout.write(f'Window sync: source={name} host={host} db={db}')

        log_sync_event(
            'window_sync_start',
            'Starting windowed sync',
            target_table=target_table,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )

        all_dfs = []
        for source in sources:
            try:
                source_start = time.time()
                self.stdout.write(f"Window sync: fetching rows from {source.get('name')}")
                df = _fetch_maria_rows(
                    source,
                    limit=limit,
                    start_date=start_date,
                    end_date=end_date,
                )
                if not df.empty:
                    all_dfs.append(df)
                elapsed = time.time() - source_start
                self.stdout.write(
                    f"Window sync: source={source.get('name')} rows={len(df)} elapsed={elapsed:.2f}s"
                )
                log_sync_event(
                    'window_source_success',
                    'Fetched rows from source',
                    source=source.get('name'),
                    rows=len(df),
                )
            except Exception as exc:
                logger.exception('Window sync: source failed: %s', source.get('name'))
                self.stdout.write(self.style.ERROR(
                    f"Window sync: source={source.get('name')} error={exc}"
                ))
                log_sync_event(
                    'window_source_error',
                    'Source fetch failed',
                    source=source.get('name'),
                    error=str(exc),
                )

        if not all_dfs:
            self.stdout.write(self.style.WARNING('No rows returned from MariaDB.'))
            log_sync_event('window_sync_no_data', 'No data fetched from any source')
            return

        df = pd.concat(all_dfs, ignore_index=True)
        self.stdout.write(f'Window sync: total rows fetched={len(df)}')

        sort_cols = [c for c in ['rs_username', 'UserServiceID', 'CreateDate'] if c in df.columns]
        if sort_cols:
            ascending = [True] * len(sort_cols)
            if 'CreateDate' in sort_cols:
                ascending[sort_cols.index('CreateDate')] = False
            df = df.sort_values(by=sort_cols, ascending=ascending, na_position='last')
            self.stdout.write(f"Window sync: sorted by {', '.join(sort_cols)}")

        df = df.reset_index(drop=True)
        df['id'] = range(1, len(df) + 1)

        ordered_cols = [
            'id',
            'CreateDate',
            'rs_userid',
            'rs_username',
            'rs_name',
            'UserServiceID',
            'username',
            'ServiceName',
            'ServicePrice',
            'Package',
            'ServiceStatus',
            'StartDate',
            'EndDate',
        ]
        ordered_cols = [c for c in ordered_cols if c in df.columns]
        df = df[ordered_cols]
        self.stdout.write(f"Window sync: columns={', '.join(ordered_cols)}")

        client = bigquery.Client(project=project)
        stage_table = f"{project}.{dataset}.report_user_service_stage_{uuid.uuid4().hex}"
        self.stdout.write(f'Window sync: stage_table={stage_table}')
        log_sync_event('window_stage_create', 'Created stage table id', stage_table=stage_table)

        try:
            self.stdout.write('Window sync: loading stage table')
            stage_start = time.time()
            _load_df_to_bq(df, stage_table, client, location)
            self.stdout.write(f'Window sync: stage load complete elapsed={time.time() - stage_start:.2f}s')

            delete_where = 'CreateDate >= DATE(@start_date)'
            if end_date:
                delete_where += ' AND CreateDate <= DATE(@end_date)'

            cols_csv = ', '.join(ordered_cols)
            query = f"""
BEGIN
  DELETE FROM `{target_table}`
  WHERE {delete_where};

  INSERT INTO `{target_table}` ({cols_csv})
  SELECT {cols_csv}
  FROM `{stage_table}`;
END;
"""
            params = [bigquery.ScalarQueryParameter('start_date', 'DATE', start_date)]
            if end_date:
                params.append(bigquery.ScalarQueryParameter('end_date', 'DATE', end_date))
            job_config = bigquery.QueryJobConfig(query_parameters=params)

            self.stdout.write('Window sync: executing delete/insert on target')
            query_job = client.query(query, job_config=job_config, location=location)
            query_job.result()
            self.stdout.write('Window sync: target update complete')

            log_sync_event('window_sync_success', 'Windowed sync completed', rows=len(df))
            elapsed = time.time() - started_at
            self.stdout.write(self.style.SUCCESS(
                f'Synced {len(df)} rows into {target_table} in {elapsed:.2f}s'
            ))
        finally:
            self.stdout.write('Window sync: cleaning up stage table')
            client.delete_table(stage_table, not_found_ok=True)
