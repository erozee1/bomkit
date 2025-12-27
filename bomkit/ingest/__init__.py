"""BOM snapshot ingestion module for database persistence."""

from .snapshot_ingest import (
    ingest_bom_snapshot,
    NormalizedRow,
    DatabaseClient,
    normalize_row_from_dict
)
from .supabase_client import SupabaseClient

# NOTE: diff types are NOT re-exported here to avoid circular imports.
# Import diff types from bomkit.diff instead:
#   from bomkit.diff import diff_snapshots, DiffResult, ...

__all__ = [
    "ingest_bom_snapshot",
    "NormalizedRow",
    "DatabaseClient",
    "SupabaseClient",
    "normalize_row_from_dict",
]


