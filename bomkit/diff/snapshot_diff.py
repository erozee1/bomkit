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
from typing import Dict, List, Optional, Any, Union, Tuple
from uuid import UUID
import json
import hashlib

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
    part_id: Optional[UUID] = None  # Part ID for semantic matching


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


def _create_semantic_key(part_id: Optional[UUID], quantity: Optional[Union[int, float]], attributes: Dict[str, Any]) -> str:
    """
    Create a semantic key for matching items across snapshots.
    
    Uses part_id + quantity + semantic attributes to identify the same logical item
    even if it has a different bom_item_id.
    
    Args:
        part_id: Part ID (None if not available)
        quantity: Quantity
        attributes: Attributes dict (will be filtered to semantic only)
        
    Returns:
        String key for semantic matching
    """
    # Filter to semantic attributes only
    semantic_attrs = _filter_semantic_attributes(attributes)
    
    # Create a stable representation
    key_parts = [
        str(part_id) if part_id else "NO_PART",
        str(quantity) if quantity is not None else "NO_QTY",
        json.dumps(semantic_attrs, sort_keys=True) if semantic_attrs else "{}"
    ]
    
    # Hash for consistent key format
    key_str = "|".join(key_parts)
    return hashlib.md5(key_str.encode()).hexdigest()


def _create_part_based_key(part_id: Optional[UUID]) -> str:
    """
    Create a key based only on part_id for matching items that represent the same part
    but may have different quantities or attributes (these should be modified, not removed+added).
    
    Args:
        part_id: Part ID (None if not available)
        
    Returns:
        String key for part-based matching
    """
    return str(part_id) if part_id else "NO_PART"


def fetch_snapshot_state(
    db: DatabaseClient,
    snapshot_id: UUID,
    bom_item_details: Optional[Dict[UUID, Dict[str, Any]]] = None
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
        bom_item_details: Optional pre-fetched bom_item details dict (for performance)
        
    Returns:
        Dictionary mapping bom_item_id -> SnapshotItemState
    """
    snapshot_items = db.get_snapshot_items(snapshot_id)
    
    # If bom_item_details not provided, fetch them
    if bom_item_details is None:
        bom_item_ids = [UUID(item['bom_item_id']) for item in snapshot_items]
        if bom_item_ids:
            bom_item_details = db.get_bom_item_details(bom_item_ids)
        else:
            bom_item_details = {}
    
    state = {}
    for item in snapshot_items:
        bom_item_id = UUID(item['bom_item_id'])
        
        # Get part_id from bom_item_details
        details = bom_item_details.get(bom_item_id, {})
        part_id = details.get('part_id')
        if part_id:
            part_id = UUID(part_id) if isinstance(part_id, str) else part_id
        
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
            checksum=item['checksum'],
            part_id=part_id
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


def diff_snapshots(
    snapshot_a_id: UUID,
    snapshot_b_id: UUID,
    db: DatabaseClient
) -> DiffResult:
    """
    Compare two BOM snapshots and produce a structured diff result.
    
    This implements a semantic diff engine that:
    1. Fetches snapshot states with part information
    2. Creates semantic keys (part_id + quantity + semantic attributes)
    3. Matches items by semantic key (not just bom_item_id)
    4. Detects changes via checksum comparison
    5. Performs field-level diffs for changed items
    
    This approach ensures that items with the same part and attributes
    are recognized as the same item even if they have different bom_item_ids.
    
    Args:
        snapshot_a_id: First snapshot ID (baseline)
        snapshot_b_id: Second snapshot ID (comparison)
        db: Database client
        
    Returns:
        DiffResult with added, removed, modified items and unchanged count
    """
    # Step 1: Fetch snapshot items and get all bom_item_ids
    snapshot_items_a = db.get_snapshot_items(snapshot_a_id)
    snapshot_items_b = db.get_snapshot_items(snapshot_b_id)
    
    all_bom_item_ids = set()
    for item in snapshot_items_a:
        all_bom_item_ids.add(UUID(item['bom_item_id']))
    for item in snapshot_items_b:
        all_bom_item_ids.add(UUID(item['bom_item_id']))
    
    # Step 2: Fetch bom_item_details for all items at once (performance optimization)
    bom_item_details = {}
    if all_bom_item_ids:
        bom_item_details = db.get_bom_item_details(list(all_bom_item_ids))
    
    # Step 3: Fetch snapshot states with part information
    state_a = fetch_snapshot_state(db, snapshot_a_id, bom_item_details)
    state_b = fetch_snapshot_state(db, snapshot_b_id, bom_item_details)
    
    # Step 4: Create semantic keys for all items
    # Map semantic_key -> list of (bom_item_id, state) tuples
    semantic_map_a: Dict[str, List[Tuple[UUID, SnapshotItemState]]] = {}
    semantic_map_b: Dict[str, List[Tuple[UUID, SnapshotItemState]]] = {}
    
    # Also create part-based maps for matching items with same part_id but different quantities/attributes
    part_map_a: Dict[str, List[Tuple[UUID, SnapshotItemState]]] = {}
    part_map_b: Dict[str, List[Tuple[UUID, SnapshotItemState]]] = {}
    
    for bom_item_id, state in state_a.items():
        semantic_key = _create_semantic_key(state.part_id, state.quantity, state.attributes)
        if semantic_key not in semantic_map_a:
            semantic_map_a[semantic_key] = []
        semantic_map_a[semantic_key].append((bom_item_id, state))
        
        # Also index by part_id for part-based matching
        part_key = _create_part_based_key(state.part_id)
        if part_key not in part_map_a:
            part_map_a[part_key] = []
        part_map_a[part_key].append((bom_item_id, state))
    
    for bom_item_id, state in state_b.items():
        semantic_key = _create_semantic_key(state.part_id, state.quantity, state.attributes)
        if semantic_key not in semantic_map_b:
            semantic_map_b[semantic_key] = []
        semantic_map_b[semantic_key].append((bom_item_id, state))
        
        # Also index by part_id for part-based matching
        part_key = _create_part_based_key(state.part_id)
        if part_key not in part_map_b:
            part_map_b[part_key] = []
        part_map_b[part_key].append((bom_item_id, state))
    
    # Step 5: Match items by semantic key
    # Track which items have been matched
    matched_a = set()
    matched_b = set()
    matched_pairs: List[Tuple[UUID, UUID]] = []  # (bom_item_id_a, bom_item_id_b)
    
    # First, try exact bom_item_id matches (fast path)
    # Items with same bom_item_id are matched regardless of checksum
    # (they'll be classified as modified or unchanged later based on checksum)
    common_bom_item_ids = set(state_a.keys()) & set(state_b.keys())
    for bom_item_id in common_bom_item_ids:
        matched_a.add(bom_item_id)
        matched_b.add(bom_item_id)
        matched_pairs.append((bom_item_id, bom_item_id))
    
    # Then, match by semantic key for unmatched items (exact matches: part_id + quantity + attributes)
    # Items with same semantic key should be matched even if they have different bom_item_ids
    for semantic_key in set(semantic_map_a.keys()) | set(semantic_map_b.keys()):
        items_a = semantic_map_a.get(semantic_key, [])
        items_b = semantic_map_b.get(semantic_key, [])
        
        # Filter out already matched items
        items_a_unmatched = [(bid, s) for bid, s in items_a if bid not in matched_a]
        items_b_unmatched = [(bid, s) for bid, s in items_b if bid not in matched_b]
        
        # Match items with same semantic key
        # Match all items with same semantic key, preferring checksum matches
        # but still matching even if checksums differ (they'll be marked as modified)
        while items_a_unmatched and items_b_unmatched:
            # Find best match: prefer exact checksum match, then any match
            best_match_idx_a = None
            best_match_idx_b = None
            best_is_exact = False
            
            for idx_a, (bid_a, state_a_item) in enumerate(items_a_unmatched):
                for idx_b, (bid_b, state_b_item) in enumerate(items_b_unmatched):
                    is_exact = state_a_item.checksum == state_b_item.checksum
                    
                    # Prefer exact checksum match
                    if is_exact and not best_is_exact:
                        best_match_idx_a = idx_a
                        best_match_idx_b = idx_b
                        best_is_exact = True
                    elif best_match_idx_a is None:
                        # Keep first potential match as fallback
                        best_match_idx_a = idx_a
                        best_match_idx_b = idx_b
                        best_is_exact = False
            
            if best_match_idx_a is not None and best_match_idx_b is not None:
                bom_item_id_a, state_a_item = items_a_unmatched[best_match_idx_a]
                bom_item_id_b, state_b_item = items_b_unmatched[best_match_idx_b]
                
                matched_a.add(bom_item_id_a)
                matched_b.add(bom_item_id_b)
                matched_pairs.append((bom_item_id_a, bom_item_id_b))
                
                # Remove matched items from lists
                items_a_unmatched.pop(best_match_idx_a)
                # Adjust index if we removed an item before the second one
                if best_match_idx_b > best_match_idx_a:
                    best_match_idx_b -= 1
                items_b_unmatched.pop(best_match_idx_b)
            else:
                # No more matches possible
                break
    
    # Finally, match by part_id only for remaining unmatched items
    # Items with same part_id but different quantities/attributes should be matched as modified
    # This handles cases where quantity or attributes changed but it's the same part
    for part_key in set(part_map_a.keys()) | set(part_map_b.keys()):
        items_a = part_map_a.get(part_key, [])
        items_b = part_map_b.get(part_key, [])
        
        # Filter out already matched items
        items_a_unmatched = [(bid, s) for bid, s in items_a if bid not in matched_a]
        items_b_unmatched = [(bid, s) for bid, s in items_b if bid not in matched_b]
        
        # Match items with same part_id (even if quantity/attributes differ)
        # This ensures items with same part are marked as modified, not removed+added
        while items_a_unmatched and items_b_unmatched:
            # Match first available pair (they'll be marked as modified if checksums differ)
            bom_item_id_a, state_a_item = items_a_unmatched[0]
            bom_item_id_b, state_b_item = items_b_unmatched[0]
            
            matched_a.add(bom_item_id_a)
            matched_b.add(bom_item_id_b)
            matched_pairs.append((bom_item_id_a, bom_item_id_b))
            
            # Remove matched items
            items_a_unmatched.pop(0)
            items_b_unmatched.pop(0)
    
    # Step 6: Classify items
    # Only items that couldn't be matched at all are truly added/removed
    added_ids = set(state_b.keys()) - matched_b
    removed_ids = set(state_a.keys()) - matched_a
    
    # Step 7: Find modified items (matched but checksum differs)
    # This includes both bom_item_id matches and semantic matches
    modified_items = []
    for bom_item_id_a, bom_item_id_b in matched_pairs:
        state_a_item = state_a[bom_item_id_a]
        state_b_item = state_b[bom_item_id_b]
        
        if state_a_item.checksum != state_b_item.checksum:
            changes = diff_snapshot_item(state_a_item, state_b_item)
            if changes:
                # Use bom_item_id from snapshot B (the newer one)
                modified_items.append(ModifiedItem(
                    bom_item_id=bom_item_id_b,
                    changes=changes
                ))
    
    # Step 8: Calculate unchanged count
    # Only items with matching checksums are truly unchanged
    unchanged_count = sum(
        1 for bom_item_id_a, bom_item_id_b in matched_pairs
        if state_a[bom_item_id_a].checksum == state_b[bom_item_id_b].checksum
    )
    
    # Return structured result
    return DiffResult(
        snapshot_a_id=snapshot_a_id,
        snapshot_b_id=snapshot_b_id,
        added_items=sorted(list(added_ids)),
        removed_items=sorted(list(removed_ids)),
        modified_items=modified_items,
        unchanged_count=unchanged_count
    )

