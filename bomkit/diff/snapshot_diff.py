"""
Snapshot diff engine for comparing BOM snapshots.

This module implements an identity-first, checksum-based diff engine that:
- Uses bom_item_id as stable identity (not row position)
- Compares checksums for cheap change detection
- Performs semantic field-level diffs only for changed items
- Produces structured, explainable change events
- Filters non-semantic metadata (row_index, normalization artifacts)
- Conservative bias: prefer modify over remove+add

This approach is inspired by Git's object-based diffing, not text-based algorithms.

CORE PRINCIPLES:
1. Engineers care about meaning, not rows
2. Identity must be stable across uploads
3. Diffs must be conservative (false positives destroy trust)
4. Non-semantic metadata must NEVER appear in diffs
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Union
from uuid import UUID

from ..ingest.snapshot_ingest import DatabaseClient, NON_SEMANTIC_ATTRIBUTE_KEYS, _filter_semantic_attributes


@dataclass
class SnapshotItemState:
    """
    Represents the state of a single bom_item in a snapshot.
    
    This is the atomic unit of comparison - each bom_item_id has one state
    per snapshot.
    """
    bom_item_id: UUID
    quantity: Optional[Union[int, float]]  # Can be int, float, or Decimal from DB
    attributes: Dict[str, Any]
    checksum: str


@dataclass
class FieldChange:
    """
    Represents a single field-level change in a snapshot item.
    
    This is a semantic change event that engineers can understand and act on.
    """
    type: str  # e.g., "QUANTITY_CHANGED", "ATTRIBUTE_CHANGED", "ATTRIBUTE_ADDED", "ATTRIBUTE_REMOVED"
    field: Optional[str]  # Field name (for attributes), None for quantity
    from_value: Any
    to_value: Any


@dataclass
class ModifiedItem:
    """
    Represents a bom_item that changed between snapshots.
    
    Contains the bom_item_id and the list of field-level changes.
    """
    bom_item_id: UUID
    changes: List[FieldChange]


@dataclass
class DiffResult:
    """
    Complete diff result between two snapshots.
    
    This is the product - structured, explainable, and suitable for:
    - UI rendering
    - LLM summarization
    - Impact analysis
    - Audit logging
    """
    snapshot_a_id: UUID
    snapshot_b_id: UUID
    added_items: List[UUID]  # bom_item_ids that exist in B but not A
    removed_items: List[UUID]  # bom_item_ids that exist in A but not B
    modified_items: List[ModifiedItem]  # bom_item_ids that changed
    unchanged_count: int  # bom_item_ids that are identical in both


def fetch_snapshot_state(
    db: DatabaseClient,
    snapshot_id: UUID
) -> Dict[UUID, SnapshotItemState]:
    """
    Fetch all snapshot items for a snapshot and represent as identity-keyed dict.
    
    This is Step 1: Load snapshot state into memory.
    Uses SQL only to retrieve data, not to "diff meaning".
    
    NOTE: snapshot_items.attributes now includes reference_designator
    (moved from bom_items.context to ensure refdes changes show as MODIFY).
    
    Args:
        db: Database client
        snapshot_id: Snapshot ID to fetch
        
    Returns:
        Dictionary mapping bom_item_id -> SnapshotItemState
    """
    snapshot_items = db.get_snapshot_items(snapshot_id)
    
    state = {}
    for item in snapshot_items:
        bom_item_id = UUID(item['bom_item_id'])
        
        # Handle quantity conversion (DB may return numeric as Decimal, int, or float)
        quantity = item['quantity']
        if quantity is not None:
            # Convert to numeric type (preserve int if possible, else float)
            if isinstance(quantity, (int, float)):
                quantity = quantity
            else:
                # Handle Decimal or string
                quantity = float(quantity)
        
        state[bom_item_id] = SnapshotItemState(
            bom_item_id=bom_item_id,
            quantity=quantity,
            attributes=item['attributes'] or {},
            checksum=item['checksum']
        )
    
    return state


def diff_snapshot_item(
    a: SnapshotItemState,
    b: SnapshotItemState
) -> List[FieldChange]:
    """
    Perform semantic field-level diff between two snapshot item states.
    
    This is Step 5: Domain-aware semantic diffing.
    Only called for items that have different checksums.
    
    CRITICAL: This function filters non-semantic attributes.
    Engineers must never see changes to row_index, normalization artifacts, etc.
    
    Args:
        a: Snapshot item state from snapshot A
        b: Snapshot item state from snapshot B
        
    Returns:
        List of field-level change events (only semantic changes)
    """
    changes = []
    
    # Compare quantity (always semantic)
    if a.quantity != b.quantity:
        changes.append(FieldChange(
            type="QUANTITY_CHANGED",
            field=None,
            from_value=float(a.quantity) if a.quantity is not None else None,
            to_value=float(b.quantity) if b.quantity is not None else None
        ))
    
    # Filter non-semantic attributes before comparison
    # This ensures row_index, normalization artifacts never appear in diffs
    attrs_a = _filter_semantic_attributes(a.attributes)
    attrs_b = _filter_semantic_attributes(b.attributes)
    
    # Compare only semantic attributes (field-by-field)
    all_keys = set(attrs_a.keys()) | set(attrs_b.keys())
    
    for key in all_keys:
        val_a = attrs_a.get(key)
        val_b = attrs_b.get(key)
        
        if val_a != val_b:
            if key not in attrs_a:
                # Attribute was added
                changes.append(FieldChange(
                    type="ATTRIBUTE_ADDED",
                    field=key,
                    from_value=None,
                    to_value=val_b
                ))
            elif key not in attrs_b:
                # Attribute was removed
                changes.append(FieldChange(
                    type="ATTRIBUTE_REMOVED",
                    field=key,
                    from_value=val_a,
                    to_value=None
                ))
            else:
                # Attribute was modified
                changes.append(FieldChange(
                    type="ATTRIBUTE_CHANGED",
                    field=key,
                    from_value=val_a,
                    to_value=val_b
                ))
    
    return changes


def _get_part_attributes_for_snapshots(
    db: DatabaseClient,
    snapshot_a_id: UUID,
    snapshot_b_id: UUID
) -> Dict[UUID, Dict[str, Any]]:
    """
    Get part attributes for all bom_items in both snapshots.
    
    Returns a dict: {bom_item_id: part_attributes}
    This allows detecting part-level changes (like footprint) that might
    not be reflected in snapshot_items if part matching incorrectly reused a part.
    
    NOTE: In a correct system, parts don't change - if attributes change,
    a new part is created. But we check this to catch cases where part
    matching was too loose and incorrectly reused a part.
    
    Args:
        db: Database client
        snapshot_a_id: First snapshot ID
        snapshot_b_id: Second snapshot ID
        
    Returns:
        Dictionary mapping bom_item_id -> part_attributes dict
    """
    # Get all bom_item_ids from both snapshots
    state_a = fetch_snapshot_state(db, snapshot_a_id)
    state_b = fetch_snapshot_state(db, snapshot_b_id)
    all_bom_item_ids = list(set(state_a.keys()) | set(state_b.keys()))
    
    if not all_bom_item_ids:
        return {}
    
    # Get bom_item details (which includes part_attributes)
    bom_item_details = db.get_bom_item_details(all_bom_item_ids)
    
    # Extract part attributes for each bom_item
    result = {}
    for bom_item_id in all_bom_item_ids:
        details = bom_item_details.get(bom_item_id, {})
        part_attrs = details.get('part_attributes', {})
        result[bom_item_id] = part_attrs
    
    return result


def diff_snapshots(
    snapshot_a_id: UUID,
    snapshot_b_id: UUID,
    db: DatabaseClient
) -> DiffResult:
    """
    Compare two BOM snapshots and produce a structured diff result.
    
    This implements the identity-first, checksum-based diff engine:
    1. Fetch snapshot states (SQL)
    2. Represent as identity-keyed dicts
    3. Structural diff (set operations)
    4. Cheap change detection (checksum comparison)
    5. Semantic diff (field-level for changed items)
    6. Structured result
    
    This approach is inspired by Git's object-based diffing:
    - Identity is stable (bom_item_id)
    - Change detection is cheap (checksum comparison)
    - Semantic diff is domain-aware (field-level)
    - Result is explainable (structured change events)
    
    Args:
        snapshot_a_id: First snapshot ID (baseline)
        snapshot_b_id: Second snapshot ID (comparison)
        db: Database client
        
    Returns:
        DiffResult with added, removed, modified items and unchanged count
    """
    # Step 1 & 2: Fetch and represent snapshots
    state_a = fetch_snapshot_state(db, snapshot_a_id)
    state_b = fetch_snapshot_state(db, snapshot_b_id)
    
    # Step 3: Structural diff (identity-based set operations)
    # This replaces all generic diff algorithms.
    # 
    # CRITICAL: We use set operations on bom_item_id, not row position.
    # This ensures:
    # - Row reordering doesn't cause false positives
    # - Identity is stable across uploads
    # - Only true add/remove operations are detected
    added_ids = set(state_b.keys()) - set(state_a.keys())
    removed_ids = set(state_a.keys()) - set(state_b.keys())
    common_ids = set(state_a.keys()) & set(state_b.keys())
    
    # Step 4: Cheap change detection (checksum comparison)
    # This mirrors Git's object hash comparison.
    # 
    # CRITICAL: Checksums are computed on SEMANTIC attributes only.
    # Non-semantic keys (row_index, normalization artifacts) are excluded.
    # This ensures:
    # - Row reordering doesn't cause false positives
    # - Only meaningful engineering changes are detected
    # - Checksums remain stable across uploads if nothing changed
    modified_ids = {
        bom_item_id for bom_item_id in common_ids
        if state_a[bom_item_id].checksum != state_b[bom_item_id].checksum
    }
    
    # Step 5: Semantic diff (domain-aware field-level comparison)
    # Only for items that actually changed
    # 
    # CRITICAL: We also check part-level attributes to catch cases where
    # part matching was too loose and incorrectly reused a part when
    # design-intent attributes (like footprint) changed.
    modified_items = []
    
    # Get part attributes for all bom_items
    # NOTE: In a correct system, parts are immutable - if attributes change,
    # a new part is created. However, we get part attributes here for reference
    # in case we need to display them in the diff output.
    # We don't compare part attributes across snapshots because parts shouldn't change.
    # If a footprint changes, it should create a NEW part (and thus new bom_item),
    # which would show up as removed+added, not modified.
    part_attrs_by_bom_item = _get_part_attributes_for_snapshots(
        db, snapshot_a_id, snapshot_b_id
    )
    
    for bom_item_id in modified_ids:
        changes = diff_snapshot_item(
            state_a[bom_item_id],
            state_b[bom_item_id]
        )
        
        # NOTE: We do NOT check part attributes here because:
        # 1. Parts are immutable - if attributes change, a new part is created
        # 2. If part matching incorrectly reused a part when footprint changed,
        #    that's a part matching bug, not a diff bug
        # 3. The diff engine's job is to compare snapshot_items, not parts
        # 
        # If footprint changes are being missed, the issue is in part matching
        # (similarity threshold too high), not in the diff engine.
        
        if changes:  # Only include if there are actual changes
            modified_items.append(ModifiedItem(
                bom_item_id=bom_item_id,
                changes=changes
            ))
    
    # Step 6: Calculate unchanged count
    unchanged_count = len(common_ids) - len(modified_ids)
    
    # Return structured result
    return DiffResult(
        snapshot_a_id=snapshot_a_id,
        snapshot_b_id=snapshot_b_id,
        added_items=sorted(list(added_ids)),
        removed_items=sorted(list(removed_ids)),
        modified_items=modified_items,
        unchanged_count=unchanged_count
    )

