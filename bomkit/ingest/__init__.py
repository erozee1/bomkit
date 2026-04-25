"""BOM snapshot ingestion module for database persistence."""

from .snapshot_ingest import (
    ingest_bom_snapshot,
    NormalizedRow,
    DatabaseClient,
    normalize_row_from_dict
)
from .supabase_client import SupabaseClient
from bomkit.diff import diff_snapshots, DiffResult

# NOTE: diff types are re-exported for backward compatibility with tests.

__all__ = [
    "ingest_bom_snapshot",
    "NormalizedRow",
    "DatabaseClient",
    "SupabaseClient",
    "normalize_row_from_dict",
    "diff_snapshots",
    "DiffResult",
]

