import os
import json
import logging
import tempfile
import uuid
from datetime import datetime, timezone
from google.cloud import bigquery
import pandas as pd
import pymysql

from pymysql.cursors import DictCursor

logger = logging.getLogger(__name__)
LOG_PATH = os.getenv('SYNC_LOG_PATH') or os.path.join(os.path.dirname(__file__), 'sync_logs.jsonl')


def _write_sync_log(entry):
    log_dir = os.path.dirname(LOG_PATH)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
    with open(LOG_PATH, 'a', encoding='utf-8') as handle:
        handle.write(json.dumps(entry, ensure_ascii=True) + '\n')


def log_sync_event(event_type, message, **data):
    entry = {
        'ts': datetime.now(timezone.utc).isoformat(),
        'type': event_type,
        'message': message,
        'data': data,
    }
    try:
        _write_sync_log(entry)
    except Exception:
        logger.exception('Sync: failed to write log event')


def read_sync_logs(limit=200):
    if limit <= 0:
        return []
    if not os.path.exists(LOG_PATH):
        return []
    with open(LOG_PATH, 'r', encoding='utf-8') as handle:
        lines = handle.readlines()
    tail = lines[-limit:]
    entries = []
    for line in tail:
        line = line.strip()
        if not line:
            continue
        try:
            entries.append(json.loads(line))
        except json.JSONDecodeError:
            entries.append({'ts': None, 'type': 'raw', 'message': line, 'data': {}})
    return entries


def _parse_sources():
    sources_raw = os.getenv('MARIA_SOURCES', '').strip()
    if sources_raw:
        sources = []
        for item in sources_raw.split(';'):
            item = item.strip()
            if not item:
                continue
            parts = [p.strip() for p in item.split(',')]
            if len(parts) < 6:
                continue
            name, host, port, db, user, password = parts[:6]
            sources.append({
                'name': name or host,
                'host': host,
                'port': int(port),
                'db': db,
                'user': user,
                'password': password,
            })
        if sources:
            return sources

    # fallback to single-source env vars
    return [{
        'name': os.getenv('MARIA_DB', 'default'),
        'host': os.getenv('MARIA_HOST', 'localhost'),
        'port': int(os.getenv('MARIA_PORT', 3306)),
        'db': os.getenv('MARIA_DB', ''),
        'user': os.getenv('MARIA_USER', 'root'),
        'password': os.getenv('MARIA_PASSWORD', ''),
    }]


def _fetch_maria_rows(source, limit=0, days=None, start_date=None, end_date=None):
    logger.info("Sync: fetching rows from %s (%s:%s/%s)", source.get('name'), source.get('host'), source.get('port'), source.get('db'))
    query = """
SELECT
    DATE(TName.CDT) AS CreateDate,
    TName.Creator_Id AS rs_userid,
    Hrc.ResellerName AS rs_username,
    Hrc.ResellerName AS rs_name,
    TName.User_ServiceBase_Id AS UserServiceID,
    Hu.Username AS username,
    Hse.ServiceName AS ServiceName,
    TName.ServicePrice AS ServicePrice,
    CASE
        WHEN COALESCE(NULLIF(Hse.STrA, 0), NULLIF(Hse.MTrA, 0), NULLIF(Hse.DTrA, 0), NULLIF(Hse.YTrA, 0), NULLIF(Hse.ExtraTraffic, 0)) IS NULL THEN NULL
        ELSE ROUND(COALESCE(NULLIF(Hse.STrA, 0), NULLIF(Hse.MTrA, 0), NULLIF(Hse.DTrA, 0), NULLIF(Hse.YTrA, 0), NULLIF(Hse.ExtraTraffic, 0)) / 1073741824, 2)
    END AS Package,
    TName.ServiceStatus AS ServiceStatus,
    DATE_FORMAT(NULLIF(TName.StartDate, '0000-00-00'), '%%Y-%%m-%%d') AS StartDate,
    DATE_FORMAT(NULLIF(TName.EndDate, '0000-00-00'), '%%Y-%%m-%%d') AS EndDate
FROM Huser_servicebase TName
JOIN Huser Hu ON TName.User_Id = Hu.User_Id
LEFT JOIN Hreseller Hrc ON TName.Creator_Id = Hrc.Reseller_Id
LEFT JOIN Hservice Hse ON TName.Service_Id = Hse.Service_Id
"""

    filters = []
    params = []
    if days is not None:
        try:
            days_val = int(days)
        except (TypeError, ValueError):
            days_val = 0
        if days_val > 0:
            filters.append("TName.CDT >= DATE_SUB(CURDATE(), INTERVAL %s DAY)")
            params.append(days_val)

    if start_date:
        filters.append("TName.CDT >= %s")
        params.append(start_date)

    if end_date:
        filters.append("TName.CDT <= %s")
        params.append(end_date)

    if filters:
        query += "\nWHERE " + " AND ".join(filters)

    query += "\nORDER BY TName.CDT DESC"
    if limit and limit > 0:
        query += f"\nLIMIT {int(limit)}"

    conn = pymysql.connect(
        host=source['host'],
        port=source['port'],
        user=source['user'],
        password=source['password'],
        db=source['db'],
        charset='utf8mb4',
        cursorclass=DictCursor,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
            df = pd.DataFrame(rows)
            if not df.empty:
                df['rs_name'] = source['name']
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
                df = df[ordered_cols]
            logger.info("Sync: fetched %s rows from %s", len(df), source.get('name'))
            return df
    except Exception:
        logger.exception("Sync: failed to fetch rows from %s", source.get('name'))
        raise
    finally:
        conn.close()


def _fetch_reseller_map(source):
    logger.info("Sync: fetching reseller map from %s", source.get('name'))
    query = """
SELECT Reseller_Id, ResellerName
FROM Hreseller
WHERE ResellerName IS NOT NULL AND ResellerName <> ''
"""
    conn = pymysql.connect(
        host=source['host'],
        port=source['port'],
        user=source['user'],
        password=source['password'],
        db=source['db'],
        charset='utf8mb4',
        cursorclass=DictCursor,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            df = pd.DataFrame(rows)
            if df.empty:
                return df
            df['creator_norm'] = df['ResellerName'].astype(str).str.strip().str.lower()
            df['rs_userid'] = df['Reseller_Id']
            df['rs_name'] = source.get('name')
            return df[['creator_norm', 'rs_userid', 'rs_name']]
    except Exception:
        logger.exception("Sync: failed to fetch reseller map from %s", source.get('name'))
        raise
    finally:
        conn.close()


def sync_maria_to_bigquery(limit=0, write_disposition='WRITE_TRUNCATE', days=None, auto=False):
    project = os.getenv('BQ_PROJECT')
    dataset = os.getenv('BQ_DATASET')
    table = os.getenv('BQ_TABLE')
    location = os.getenv('BQ_LOCATION', 'US')
    if not (project and dataset and table):
        raise RuntimeError('BigQuery config missing: BQ_PROJECT, BQ_DATASET, BQ_TABLE')

    table_id = f"{project}.{dataset}.{table}"

    sources = _parse_sources()
    source_names = [s.get('name') for s in sources]
    logger.info("Sync: starting BigQuery load to %s", table_id)
    log_sync_event(
        'sync_start',
        'Starting BigQuery sync',
        table_id=table_id,
        sources=source_names,
        limit=limit,
        days=days,
        auto=auto,
        write_disposition=write_disposition,
    )
    all_dfs = []
    for source in sources:
        try:
            log_sync_event(
                'source_start',
                'Fetching rows from source',
                source=source.get('name'),
                host=source.get('host'),
                db=source.get('db'),
            )
            df = _fetch_maria_rows(source, limit=limit, days=days)
            log_sync_event(
                'source_success',
                'Fetched rows from source',
                source=source.get('name'),
                rows=len(df),
            )
            if not df.empty:
                all_dfs.append(df)
        except Exception as exc:
            logger.exception("Sync: source failed: %s", source.get('name'))
            log_sync_event(
                'source_error',
                'Source fetch failed',
                source=source.get('name'),
                error=str(exc),
            )

    if not all_dfs:
        logger.warning("Sync: no data fetched from any source")
        log_sync_event('sync_no_data', 'No data fetched from any source')
        return 0

    df = pd.concat(all_dfs, ignore_index=True)

    # sort by reseller username then date for easier grouping in BigQuery
    sort_cols = [c for c in ['rs_username', 'UserServiceID', 'CreateDate'] if c in df.columns]
    if sort_cols:
        ascending = [True] * len(sort_cols)
        if 'CreateDate' in sort_cols:
            ascending[sort_cols.index('CreateDate')] = False
        df = df.sort_values(by=sort_cols, ascending=ascending, na_position='last')

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
    if ordered_cols:
        df = df[ordered_cols]

    client = bigquery.Client(project=project)
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        autodetect=True,
        source_format=bigquery.SourceFormat.CSV,
    )

    try:
        with tempfile.NamedTemporaryFile(mode='w+', suffix='.csv', delete=False, encoding='utf-8') as tmp:
            df.to_csv(tmp.name, index=False)
            tmp.flush()
            tmp.seek(0)
            with open(tmp.name, 'rb') as fh:
                load_job = client.load_table_from_file(
                    fh,
                    table_id,
                    job_config=job_config,
                    location=location,
                )
                load_job.result()
                logger.info("Sync: loaded %s rows into %s", len(df), table_id)
        log_sync_event('sync_loaded', 'Loaded rows into BigQuery', rows=len(df), table_id=table_id, auto=auto)
        return len(df)
    except Exception as exc:
        log_sync_event('sync_error', 'BigQuery load failed', table_id=table_id, error=str(exc), auto=auto)
        raise
