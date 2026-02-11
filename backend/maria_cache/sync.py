import logging
import os

from django.db import transaction

from .models import (
    Center,
    CenterVispAccess,
    Reseller,
    ResellerPermit,
    Service,
    ServiceResellerAccess,
    ServiceVispAccess,
    Status,
    StatusResellerAccess,
    StatusVispAccess,
    Supporter,
    Visp,
)

logger = logging.getLogger(__name__)


def _bool_yes(value):
    return str(value).strip().lower() == 'yes'


def _fetch_rows(conn, query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()


def _replace_for_source(model, source_name, rows, builder, db_alias='cache', dry_run=False, batch_size=None):
    if dry_run:
        return len(rows)
    model.objects.using(db_alias).filter(source_name=source_name).delete()
    if not rows:
        return 0
    items = [builder(row) for row in rows]
    model.objects.using(db_alias).bulk_create(items, ignore_conflicts=True, batch_size=batch_size)
    return len(items)


def sync_reference_tables(source_name=None, dry_run=False, limit=None, verbose=False):
    from reports.db import get_conn, get_sources
    sources = get_sources()
    if source_name:
        sources = [s for s in sources if s.get('name') == source_name]
    if not sources:
        raise RuntimeError('No MariaDB sources configured for cache sync.')

    summaries = []
    for source in sources:
        name = source.get('name')
        logger.info("Starting cache sync for %s", name)
        conn = get_conn(source_name=name)
        try:
            resellers = _fetch_rows(conn, "SELECT Reseller_Id, ResellerName, ISEnable FROM Hreseller")
            visps = _fetch_rows(conn, "SELECT Visp_Id, VispName, ISEnable FROM Hvisp")
            centers = _fetch_rows(conn, "SELECT Center_Id, CenterName, ISEnable, VispAccess FROM Hcenter")
            supporters = _fetch_rows(conn, "SELECT Supporter_Id, SupporterName, ISEnable FROM Hsupporter")
            statuses = _fetch_rows(conn, "SELECT Status_Id, StatusName, ISEnable, ResellerAccess, VispAccess FROM Hstatus")
            services = _fetch_rows(conn, "SELECT Service_Id, ServiceName, ISEnable, IsDel, ResellerAccess, VispAccess FROM Hservice")
            reseller_permits = _fetch_rows(conn, "SELECT Reseller_Permit_Id, Reseller_Id, Visp_Id, ISPermit, PermitItem_Id FROM Hreseller_permit")

            service_reseller = _fetch_rows(conn, "SELECT Service_ResellerAccess_Id, Service_Id, Reseller_Id, Checked FROM Hservice_reselleraccess")
            status_reseller = _fetch_rows(conn, "SELECT Status_ResellerAccess_Id, Status_Id, Reseller_Id, Checked FROM Hstatus_reselleraccess")
            service_visp = _fetch_rows(conn, "SELECT Service_VispAccess_Id, Service_Id, Visp_Id, Checked FROM Hservice_vispaccess")
            status_visp = _fetch_rows(conn, "SELECT Status_VispAccess_Id, Status_Id, Visp_Id, Checked FROM Hstatus_vispaccess")
            center_visp = _fetch_rows(conn, "SELECT Center_VispAccess_Id, Center_Id, Visp_Id, Checked FROM Hcenter_vispaccess")

            if limit and limit > 0:
                resellers = resellers[:limit]
                visps = visps[:limit]
                centers = centers[:limit]
                supporters = supporters[:limit]
                statuses = statuses[:limit]
                services = services[:limit]
                reseller_permits = reseller_permits[:limit]
                service_reseller = service_reseller[:limit]
                status_reseller = status_reseller[:limit]
                service_visp = service_visp[:limit]
                status_visp = status_visp[:limit]
                center_visp = center_visp[:limit]

            counts = {
                'resellers': len(resellers),
                'visps': len(visps),
                'centers': len(centers),
                'supporters': len(supporters),
                'statuses': len(statuses),
                'services': len(services),
                'reseller_permits': len(reseller_permits),
                'service_reseller': len(service_reseller),
                'status_reseller': len(status_reseller),
                'service_visp': len(service_visp),
                'status_visp': len(status_visp),
                'center_visp': len(center_visp),
            }
            summaries.append({'source': name, 'counts': counts, 'dry_run': dry_run})

            if verbose:
                logger.info("Counts for %s: %s", name, counts)

            if dry_run:
                logger.info("Dry-run mode enabled; skipping writes for %s", name)
                continue

            batch_size = int(os.getenv('CACHE_SYNC_BATCH_SIZE', '1000'))
            with transaction.atomic(using='cache'):
                logger.info("Syncing %s resellers", len(resellers))
                _replace_for_source(Reseller, name, resellers, lambda r: Reseller(
                    source_name=name,
                    source_id=r.get('Reseller_Id'),
                    name=r.get('ResellerName') or '',
                    name_norm=(r.get('ResellerName') or '').strip().lower(),
                    is_enabled=_bool_yes(r.get('ISEnable')),
                ), db_alias='cache', dry_run=dry_run, batch_size=batch_size)
                logger.info("Syncing %s visps", len(visps))
                _replace_for_source(Visp, name, visps, lambda r: Visp(
                    source_name=name,
                    source_id=r.get('Visp_Id'),
                    name=r.get('VispName') or '',
                    is_enabled=_bool_yes(r.get('ISEnable')),
                ), db_alias='cache', dry_run=dry_run, batch_size=batch_size)
                logger.info("Syncing %s centers", len(centers))
                _replace_for_source(Center, name, centers, lambda r: Center(
                    source_name=name,
                    source_id=r.get('Center_Id'),
                    name=r.get('CenterName') or '',
                    is_enabled=_bool_yes(r.get('ISEnable')),
                    visp_access=r.get('VispAccess') or 'All',
                ), db_alias='cache', dry_run=dry_run, batch_size=batch_size)
                logger.info("Syncing %s supporters", len(supporters))
                _replace_for_source(Supporter, name, supporters, lambda r: Supporter(
                    source_name=name,
                    source_id=r.get('Supporter_Id'),
                    name=r.get('SupporterName') or '',
                    is_enabled=_bool_yes(r.get('ISEnable')),
                ), db_alias='cache', dry_run=dry_run, batch_size=batch_size)
                logger.info("Syncing %s statuses", len(statuses))
                _replace_for_source(Status, name, statuses, lambda r: Status(
                    source_name=name,
                    source_id=r.get('Status_Id'),
                    name=r.get('StatusName') or '',
                    is_enabled=_bool_yes(r.get('ISEnable')),
                    reseller_access=r.get('ResellerAccess') or 'All',
                    visp_access=r.get('VispAccess') or 'All',
                ), db_alias='cache', dry_run=dry_run, batch_size=batch_size)
                logger.info("Syncing %s services", len(services))
                _replace_for_source(Service, name, services, lambda r: Service(
                    source_name=name,
                    source_id=r.get('Service_Id'),
                    name=r.get('ServiceName') or '',
                    is_enabled=_bool_yes(r.get('ISEnable')),
                    is_deleted=_bool_yes(r.get('IsDel')),
                    reseller_access=r.get('ResellerAccess') or 'All',
                    visp_access=r.get('VispAccess') or 'All',
                ), db_alias='cache', dry_run=dry_run, batch_size=batch_size)
                logger.info("Syncing %s reseller permits", len(reseller_permits))
                _replace_for_source(ResellerPermit, name, reseller_permits, lambda r: ResellerPermit(
                    source_name=name,
                    reseller_id=r.get('Reseller_Id') or 0,
                    visp_id=r.get('Visp_Id') or 0,
                    permit_item_id=r.get('PermitItem_Id'),
                    is_permit=_bool_yes(r.get('ISPermit')),
                ), db_alias='cache', dry_run=dry_run, batch_size=batch_size)
                logger.info("Syncing %s service-reseller access", len(service_reseller))
                _replace_for_source(ServiceResellerAccess, name, service_reseller, lambda r: ServiceResellerAccess(
                    source_name=name,
                    service_id=r.get('Service_Id') or 0,
                    reseller_id=r.get('Reseller_Id') or 0,
                    checked=_bool_yes(r.get('Checked')),
                ), db_alias='cache', dry_run=dry_run, batch_size=batch_size)
                logger.info("Syncing %s status-reseller access", len(status_reseller))
                _replace_for_source(StatusResellerAccess, name, status_reseller, lambda r: StatusResellerAccess(
                    source_name=name,
                    status_id=r.get('Status_Id') or 0,
                    reseller_id=r.get('Reseller_Id') or 0,
                    checked=_bool_yes(r.get('Checked')),
                ), db_alias='cache', dry_run=dry_run, batch_size=batch_size)
                logger.info("Syncing %s service-visp access", len(service_visp))
                _replace_for_source(ServiceVispAccess, name, service_visp, lambda r: ServiceVispAccess(
                    source_name=name,
                    service_id=r.get('Service_Id') or 0,
                    visp_id=r.get('Visp_Id') or 0,
                    checked=_bool_yes(r.get('Checked')),
                ), db_alias='cache', dry_run=dry_run, batch_size=batch_size)
                logger.info("Syncing %s status-visp access", len(status_visp))
                _replace_for_source(StatusVispAccess, name, status_visp, lambda r: StatusVispAccess(
                    source_name=name,
                    status_id=r.get('Status_Id') or 0,
                    visp_id=r.get('Visp_Id') or 0,
                    checked=_bool_yes(r.get('Checked')),
                ), db_alias='cache', dry_run=dry_run, batch_size=batch_size)
                logger.info("Syncing %s center-visp access", len(center_visp))
                _replace_for_source(CenterVispAccess, name, center_visp, lambda r: CenterVispAccess(
                    source_name=name,
                    center_id=r.get('Center_Id') or 0,
                    visp_id=r.get('Visp_Id') or 0,
                    checked=_bool_yes(r.get('Checked')),
                ), db_alias='cache', dry_run=dry_run, batch_size=batch_size)

            logger.info("Maria cache sync completed for %s", name)
        finally:
            conn.close()

    return summaries
