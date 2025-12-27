"""BOM snapshot diff module for comparing snapshots."""

from .snapshot_diff import (
    diff_snapshots,
    DiffResult,
    SnapshotItemState,
    ModifiedItem,
    FieldChange,
    fetch_snapshot_state,
    diff_snapshot_item
)

from .change_events import (
    # Main classification function
    classify_diff,
    classify_and_summarize,
    get_high_priority_events,
    get_procurement_events,
    # Enums
    ChangeEventType,
    Severity,
    Domain,
    # Data classes
    ItemDelta,
    ChangeEvent,
    ClassificationResult,
)

__all__ = [
    # Layer 1: Semantic Diff
    "diff_snapshots",
    "DiffResult",
    "SnapshotItemState",
    "ModifiedItem",
    "FieldChange",
    "fetch_snapshot_state",
    "diff_snapshot_item",
    # Layer 2: Change Event Classification
    "classify_diff",
    "classify_and_summarize",
    "get_high_priority_events",
    "get_procurement_events",
    "ChangeEventType",
    "Severity",
    "Domain",
    "ItemDelta",
    "ChangeEvent",
    "ClassificationResult",
]


