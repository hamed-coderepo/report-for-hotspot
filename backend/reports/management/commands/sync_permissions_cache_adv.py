import time
from django.core.management.base import BaseCommand, CommandError

from maria_cache.sync import sync_reference_tables
from reports.db import get_sources


class Command(BaseCommand):
    help = "Sync permission cache from MariaDB sources with optional filters."

    def add_arguments(self, parser):
        parser.add_argument('--source', type=str, default='', help='Single source name to sync.')
        parser.add_argument('--dry-run', action='store_true', help='Fetch only; do not write to cache DB.')
        parser.add_argument('--verbose', action='store_true', help='Print per-table counts for each source.')
        parser.add_argument('--limit', type=int, default=0, help='Optional row limit per table (0 = no limit).')

    def handle(self, *args, **options):
        source_name = options['source'].strip() or None
        dry_run = options['dry_run']
        verbose = options['verbose']
        limit = options['limit']

        sources = [s.get('name') for s in get_sources() if s.get('name')]
        if not sources:
            raise CommandError('No MariaDB sources configured.')

        if source_name and source_name not in sources:
            raise CommandError(f'Unknown source: {source_name}')

        target_list = [source_name] if source_name else sources
        self.stdout.write(f"Starting permission cache sync for {len(target_list)} source(s)...")

        start = time.monotonic()
        summaries = sync_reference_tables(
            source_name=source_name,
            dry_run=dry_run,
            limit=limit if limit > 0 else None,
            verbose=verbose,
        )

        if summaries and verbose:
            for item in summaries:
                source = item.get('source')
                counts = item.get('counts', {})
                self.stdout.write(f"Counts for {source}: {counts}")

        elapsed = time.monotonic() - start
        mode = 'dry-run' if dry_run else 'write'
        self.stdout.write(self.style.SUCCESS(
            f"Permission cache sync finished in {elapsed:.1f}s ({mode})."
        ))
