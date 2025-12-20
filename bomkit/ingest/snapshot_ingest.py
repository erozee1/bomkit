"""
Snapshot-based BOM ingestion engine.

This module implements a snapshot-based BOM ingestion system that separates:
- Design intent (parts): What the part is (intrinsic specs)
- Usage context (bom_items): How it's used in an assembly (refdes, placement)
- Snapshot state (snapshot_items): What is true at a point in time (quantity, attributes)

Key principles:
- Never mutate past data
- Reuse entities across uploads
- Append-only snapshots
- Preserve ambiguity
"""

import hashlib
import json
import logging
from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Tuple
from uuid import UUID

# Configure logging for identity resolution decisions
logger = logging.getLogger(__name__)

# Non-semantic attribute keys that must NEVER appear in diffs
# 
# These are metadata, normalization artifacts, or row-level tracking data.
# Engineers must never see changes to these fields because:
# - They don't represent engineering decisions
# - They cause false positives (row reordering, CSV formatting)
# - They destroy trust in the diff system
#
# CRITICAL: Any key in this set is:
# - Excluded from checksum computation
# - Excluded from semantic diffing
# - Never shown to engineers
NON_SEMANTIC_ATTRIBUTE_KEYS = {
    "row_index",           # CSV row position (not semantic)
    "source_row",          # Original row data (not semantic)
    "raw_row",             # Raw CSV row (not semantic)
    "normalized_refdes",   # Normalization artifact (refdes is context, not identity)
    "original_refdes",     # Original refdes before normalization (not semantic)
    "csv_row_number",      # Row number in CSV (not semantic)
    "import_timestamp",    # When imported (not semantic)
    "source_file",         # Source file name (not semantic)
}


@dataclass
class NormalizedRow:
    """
    Represents a normalized BOM row from bomkit.
    
    This structure separates:
    - part_name: The identifier/name of the part
    - quantity: How many are needed (snapshot-specific)
    - attributes: Intrinsic part specs (value, tolerance, material, package)
    - context: Usage-specific info (refdes, notes, placement)
    - row_index: Original row position in CSV (for debugging)
    """
    part_name: str
    quantity: Optional[int]
    attributes: Dict[str, Any]  # value, tolerance, material, package, etc.
    context: Dict[str, Any]      # refdes, notes, placement
    row_index: int


def normalize_row_from_dict(
    row_dict: Dict[str, Any],
    row_index: int
) -> NormalizedRow:
    """
    Convert a normalized dictionary (from BomNormalizer) to a NormalizedRow.
    
    This utility function helps integrate with the existing bomkit normalizer
    which produces dictionaries with standard column names.
    
    Mapping:
    - part_name: from "part_number" or "description"
    - quantity: from "quantity" (converted to int)
    - attributes: value, tolerance, material, package, manufacturer, etc.
    - context: reference_designator, notes, placement info
    
    Args:
        row_dict: Dictionary with standard column names from BomNormalizer
        row_index: Original row index in the CSV (0-based or 1-based)
        
    Returns:
        NormalizedRow instance
    """
    # Extract part name (prefer part_number, fallback to description)
    part_name = row_dict.get("part_number", "").strip()
    if not part_name:
        part_name = row_dict.get("description", "").strip()
    if not part_name:
        part_name = "UNNAMED_PART"  # Fallback for completely empty rows
    
    # Extract quantity (convert to int, handle empty strings)
    quantity_str = row_dict.get("quantity", "").strip()
    quantity = None
    if quantity_str:
        try:
            quantity = int(float(quantity_str))  # Handle "1.0" -> 1
        except (ValueError, TypeError):
            quantity = None
    
    # Extract part attributes (intrinsic specs)
    # These go into parts.attributes
    attributes = {}
    if row_dict.get("value"):
        attributes["value"] = row_dict["value"]
    if row_dict.get("package"):
        attributes["package"] = row_dict["package"]
    if row_dict.get("manufacturer"):
        attributes["manufacturer"] = row_dict["manufacturer"]
    if row_dict.get("manufacturer_part_number"):
        attributes["manufacturer_part_number"] = row_dict["manufacturer_part_number"]
    if row_dict.get("description"):
        attributes["description"] = row_dict["description"]
    if row_dict.get("unit"):
        attributes["unit"] = row_dict["unit"]
    
    # Extract tolerance from notes if present (common pattern)
    notes = row_dict.get("notes", "")
    if notes:
        # Try to extract tolerance (e.g., "Tolerance: 5%")
        import re
        tolerance_match = re.search(r'tolerance[:\s]+([0-9.]+%)', notes, re.IGNORECASE)
        if tolerance_match:
            attributes["tolerance"] = tolerance_match.group(1)
    
    # Extract usage context (how it's used in assembly)
    # These go into bom_items.context
    context = {}
    if row_dict.get("reference_designator"):
        context["reference_designator"] = row_dict["reference_designator"]
    if row_dict.get("notes"):
        context["notes"] = row_dict["notes"]
    
    return NormalizedRow(
        part_name=part_name,
        quantity=quantity,
        attributes=attributes,
        context=context,
        row_index=row_index
    )


class DatabaseClient:
    """
    Abstract database client interface.
    
    Implement this interface with your actual database client (e.g., psycopg2, SQLAlchemy).
    All methods should use transactions and be idempotent where possible.
    """
    
    def get_or_create_organization(
        self,
        org_id: UUID,
        org_name: Optional[str] = None
    ) -> UUID:
        """
        Resolve or create an organization.
        
        Args:
            org_id: Organization ID (if it exists, returns it; if not, creates with this ID)
            org_name: Optional organization name (used when creating)
            
        Returns:
            Organization UUID (existing or newly created)
        """
        raise NotImplementedError
    
    def get_assembly_by_id(
        self,
        org_id: UUID,
        assembly_id: UUID
    ) -> UUID:
        """
        Get an existing assembly by ID, verifying it belongs to the organization.
        
        Args:
            org_id: Organization ID
            assembly_id: Assembly ID to verify
            
        Returns:
            Assembly UUID if it exists and belongs to the org
            
        Raises:
            ValueError: If assembly doesn't exist or doesn't belong to org
        """
        raise NotImplementedError
    
    def get_or_create_assembly(
        self, 
        org_id: UUID, 
        assembly_name: str
    ) -> UUID:
        """
        Resolve or create an assembly by name.
        
        Args:
            org_id: Organization ID
            assembly_name: Name of the assembly
            
        Returns:
            Assembly UUID (existing or newly created)
        """
        raise NotImplementedError
    
    def find_similar_parts(
        self,
        org_id: UUID,
        part_name: str,
        attributes: Dict[str, Any],
        similarity_threshold: float = 0.8
    ) -> List[Tuple[UUID, float]]:
        """
        Find existing parts that might match this one.
        
        Args:
            org_id: Organization ID
            part_name: Part name to match
            attributes: Part attributes to match
            similarity_threshold: Minimum confidence score (0.0-1.0)
            
        Returns:
            List of (part_id, confidence_score) tuples, sorted by confidence descending
        """
        raise NotImplementedError
    
    def create_part(
        self,
        org_id: UUID,
        part_name: str,
        attributes: Dict[str, Any]
    ) -> UUID:
        """
        Create a new part.
        
        Args:
            org_id: Organization ID
            part_name: Part name
            attributes: Part attributes (value, tolerance, material, package, etc.)
            
        Returns:
            New part UUID
        """
        raise NotImplementedError
    
    def find_similar_bom_items(
        self,
        assembly_id: UUID,
        part_id: UUID,
        context: Dict[str, Any],
        similarity_threshold: float = 0.7
    ) -> List[Tuple[UUID, float]]:
        """
        Find existing bom_items that might match this usage.
        
        Args:
            assembly_id: Assembly ID
            part_id: Part ID
            context: Usage context (refdes, notes, placement)
            similarity_threshold: Minimum confidence score (0.0-1.0)
            
        Returns:
            List of (bom_item_id, confidence_score) tuples, sorted by confidence descending
        """
        raise NotImplementedError
    
    def create_bom_item(
        self,
        assembly_id: UUID,
        part_id: UUID,
        context: Dict[str, Any]
    ) -> UUID:
        """
        Create a new bom_item (part usage in assembly).
        
        Args:
            assembly_id: Assembly ID
            part_id: Part ID
            context: Usage context (refdes, notes, placement)
            
        Returns:
            New bom_item UUID
        """
        raise NotImplementedError
    
    def create_snapshot(
        self,
        org_id: UUID,
        assembly_id: UUID,
        source: str,
        parent_snapshot_id: Optional[UUID] = None
    ) -> UUID:
        """
        Create a new snapshot (immutable).
        
        Args:
            org_id: Organization ID
            assembly_id: Assembly ID
            source: Source identifier (e.g., "csv")
            parent_snapshot_id: Optional parent snapshot for lineage
            
        Returns:
            New snapshot UUID
        """
        raise NotImplementedError
    
    def insert_snapshot_item(
        self,
        snapshot_id: UUID,
        bom_item_id: UUID,
        quantity: Optional[int],
        attributes: Dict[str, Any],
        checksum: str
    ) -> None:
        """
        Insert a snapshot_item (materialized state at snapshot time).
        
        Args:
            snapshot_id: Snapshot ID
            bom_item_id: BOM item ID
            quantity: Quantity at this snapshot
            attributes: Snapshot-local attributes (temporary notes, row_index, etc.)
            checksum: Deterministic checksum of quantity + attributes
        """
        raise NotImplementedError
    
    def begin_transaction(self) -> None:
        """Begin a database transaction."""
        raise NotImplementedError
    
    def commit_transaction(self) -> None:
        """Commit the current transaction."""
        raise NotImplementedError
    
    def rollback_transaction(self) -> None:
        """Rollback the current transaction."""
        raise NotImplementedError
    
    def get_snapshot_items(
        self,
        snapshot_id: UUID
    ) -> List[Dict[str, Any]]:
        """
        Get all snapshot items for a snapshot.
        
        Returns raw data from snapshot_items table for diffing.
        
        Args:
            snapshot_id: Snapshot ID
            
        Returns:
            List of dictionaries with bom_item_id, quantity, attributes, checksum
        """
        raise NotImplementedError
    
    def get_bom_item_details(
        self,
        bom_item_ids: List[UUID]
    ) -> Dict[UUID, Dict[str, Any]]:
        """
        Get details for bom_items including part and assembly information.
        
        Args:
            bom_item_ids: List of bom_item UUIDs
            
        Returns:
            Dictionary mapping bom_item_id -> {
                'bom_item_id': UUID,
                'assembly_id': UUID,
                'assembly_name': str,
                'part_id': UUID,
                'part_name': str,
                'part_attributes': dict,
                'context': dict
            }
        """
        raise NotImplementedError
    
    def get_snapshot_info(
        self,
        snapshot_id: UUID
    ) -> Dict[str, Any]:
        """
        Get snapshot metadata including assembly information.
        
        Args:
            snapshot_id: Snapshot ID
            
        Returns:
            Dictionary with snapshot metadata:
            {
                'snapshot_id': UUID,
                'assembly_id': UUID,
                'assembly_name': str,
                'org_id': UUID,
                'source': str,
                'created_at': datetime,
                'parent_snapshot_id': UUID or None
            }
        """
        raise NotImplementedError


def _filter_semantic_attributes(attributes: Dict[str, Any]) -> Dict[str, Any]:
    """
    Filter out non-semantic attributes that should not participate in checksums or diffs.
    
    This ensures:
    - Row reordering doesn't cause false positives
    - Normalization artifacts don't appear in diffs
    - Only meaningful engineering changes are detected
    
    Args:
        attributes: Raw attributes dictionary
        
    Returns:
        Filtered dictionary with only semantic attributes
    """
    return {
        k: v for k, v in attributes.items()
        if k not in NON_SEMANTIC_ATTRIBUTE_KEYS
    }


def _canonicalize_for_checksum(value: Any) -> Any:
    """
    Canonicalize a value for checksum computation.
    
    This normalizes trivial formatting differences (whitespace only)
    without changing semantic meaning.
    
    Args:
        value: Value to canonicalize
        
    Returns:
        Canonicalized value
    """
    if isinstance(value, str):
        # Normalize whitespace (collapse multiple spaces, strip)
        # But preserve semantic differences
        return ' '.join(value.split())
    return value


def _compute_checksum(quantity: Optional[int], attributes: Dict[str, Any]) -> str:
    """
    Compute a deterministic checksum for snapshot_item state.
    
    This checksum must:
    - Change when quantity or SEMANTIC attributes change
    - Remain stable across uploads if nothing changed
    - NOT include IDs, timestamps, or non-semantic metadata
    - Be canonicalized to avoid false positives from formatting
    
    CRITICAL: Only semantic attributes participate in checksum.
    Non-semantic keys (row_index, normalization artifacts) are excluded.
    
    NOTE ON FOOTPRINT/PACKAGE:
    Footprint/package is stored in parts.attributes (design intent), not snapshot_items.
    If footprint changes, it should create a NEW part (different part_id), which
    creates a NEW bom_item (different bom_item_id), showing as removed+added.
    If footprint changes are missed, the issue is in part matching (similarity threshold
    too high), not in checksum computation.
    
    Args:
        quantity: Quantity value
        attributes: Snapshot-local attributes (may contain non-semantic keys)
        
    Returns:
        SHA256 hex digest
    """
    # Filter non-semantic attributes
    # This ensures row_index, normalization artifacts never affect checksum
    semantic_attrs = _filter_semantic_attributes(attributes)
    
    # Canonicalize values (normalize whitespace, etc.)
    # This prevents false positives from trivial formatting differences
    canonical_attrs = {
        k: _canonicalize_for_checksum(v)
        for k, v in semantic_attrs.items()
    }
    
    # Sort keys to ensure deterministic JSON serialization
    # This ensures checksums are stable regardless of attribute insertion order
    payload = {
        "quantity": quantity,
        "attributes": canonical_attrs
    }
    json_str = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(json_str.encode('utf-8')).hexdigest()


def _extract_part_attributes(row: NormalizedRow) -> Dict[str, Any]:
    """
    Extract intrinsic part attributes from a normalized row.
    
    These are specs that define WHAT the part is, not HOW it's used.
    Goes into parts.attributes.
    
    Includes: value, tolerance, material, package, manufacturer, manufacturer_part_number
    
    Args:
        row: Normalized row
        
    Returns:
        Dictionary of part attributes
    """
    # Map from normalized row fields to part attributes
    # These are the intrinsic specs that define the part identity
    attributes = {}
    
    # Value (electrical value, rating, etc.)
    if row.attributes.get("value"):
        attributes["value"] = row.attributes["value"]
    
    # Tolerance
    if row.attributes.get("tolerance"):
        attributes["tolerance"] = row.attributes["tolerance"]
    
    # Material
    if row.attributes.get("material"):
        attributes["material"] = row.attributes["material"]
    
    # Package (footprint, case type)
    if row.attributes.get("package"):
        attributes["package"] = row.attributes["package"]
    
    # Manufacturer info (intrinsic to the part)
    if row.attributes.get("manufacturer"):
        attributes["manufacturer"] = row.attributes["manufacturer"]
    
    if row.attributes.get("manufacturer_part_number"):
        attributes["manufacturer_part_number"] = row.attributes["manufacturer_part_number"]
    
    # Description (if it's a part-level description)
    if row.attributes.get("description"):
        attributes["description"] = row.attributes["description"]
    
    # Unit (if applicable to the part itself)
    if row.attributes.get("unit"):
        attributes["unit"] = row.attributes["unit"]
    
    return attributes


def _extract_bom_item_context(row: NormalizedRow) -> Dict[str, Any]:
    """
    Extract usage context from a normalized row.
    
    These are specs that define HOW the part is used in this assembly.
    Goes into bom_items.context.
    
    CRITICAL: Reference designator is NOT included here.
    Reference designators are snapshot-specific state (can change between uploads),
    not stable identity. They belong in snapshot_items.attributes, not bom_items.context.
    
    Includes: placement notes, installation notes, torque specs
    (Stable usage context that doesn't change between snapshots)
    
    Args:
        row: Normalized row
        
    Returns:
        Dictionary of usage context (without reference_designator)
    """
    context = {}
    
    # NOTE: Reference designator is intentionally EXCLUDED from bom_items.context
    # because it's snapshot-specific state, not stable identity.
    # When refdes changes (D1-D8 → D1-D6), it should show as a MODIFY, not remove+add.
    # Storing refdes in snapshot_items.attributes achieves this.
    
    # Usage-specific notes (placement, installation, etc.)
    if row.context.get("notes"):
        context["notes"] = row.context["notes"]
    
    # Any other usage-specific context (stable across snapshots)
    if row.context.get("placement"):
        context["placement"] = row.context["placement"]
    
    if row.context.get("torque"):
        context["torque"] = row.context["torque"]
    
    if row.context.get("install_notes"):
        context["install_notes"] = row.context["install_notes"]
    
    return context


def _extract_snapshot_attributes(row: NormalizedRow) -> Dict[str, Any]:
    """
    Extract snapshot-local attributes from a normalized row.
    
    These are snapshot-specific attributes that can change between uploads.
    Goes into snapshot_items.attributes.
    
    CRITICAL: Reference designator is stored here, not in bom_items.context.
    This ensures refdes changes show as MODIFY (attribute change), not remove+add.
    
    Includes:
    - reference_designator (semantic - changes show as attribute updates)
    - row_index (non-semantic - filtered from checksums/diffs)
    - Other snapshot-specific metadata
    
    Args:
        row: Normalized row
        
    Returns:
        Dictionary of snapshot-local attributes
    """
    attributes = {}
    
    # Reference designator (snapshot-specific state)
    # CRITICAL: Storing refdes here (not in bom_items.context) ensures:
    # - Refdes changes show as MODIFY, not remove+add
    # - Same bom_item_id is reused across snapshots
    # - Identity remains stable even when refdes changes
    if row.context.get("reference_designator"):
        attributes["reference_designator"] = row.context["reference_designator"]
    
    # Row index for debugging/traceability
    # NOTE: This is NON-SEMANTIC and will be filtered from checksums/diffs
    # Engineers must never see row_index changes in diffs
    attributes["row_index"] = row.row_index
    
    # Any other snapshot-specific metadata can go here
    # (e.g., import timestamp, source file name, etc.)
    # But remember: non-semantic keys are filtered from checksums
    
    return attributes


def _resolve_or_create_part(
    db: DatabaseClient,
    org_id: UUID,
    row: NormalizedRow,
    debug: bool = False
) -> UUID:
    """
    Resolve an existing part or create a new one.
    
    This implements the DESIGN INTENT resolution:
    - Attempts to match existing parts using name similarity and attribute overlap
    - If confidence is high → reuse existing part
    - Else → create new part
    
    CRITICAL: We do NOT create a new part per row. We reuse parts across uploads.
    
    Args:
        db: Database client
        org_id: Organization ID
        row: Normalized row
        debug: Enable debug logging
        
    Returns:
        Part UUID (existing or newly created)
    """
    part_attributes = _extract_part_attributes(row)
    
    # Find similar parts
    similar_parts = db.find_similar_parts(
        org_id=org_id,
        part_name=row.part_name,
        attributes=part_attributes,
        similarity_threshold=0.8  # High threshold for part matching
    )
    
    if similar_parts:
        # Use the best match if confidence is high enough
        best_match_id, confidence = similar_parts[0]
        
        if debug:
            logger.info(
                f"Part match: '{row.part_name}' → existing part {best_match_id} "
                f"(confidence: {confidence:.2f})"
            )
        
        return best_match_id
    
    # No good match found, create new part
    part_id = db.create_part(
        org_id=org_id,
        part_name=row.part_name,
        attributes=part_attributes
    )
    
    if debug:
        logger.info(f"Part created: '{row.part_name}' → new part {part_id}")
    
    return part_id


def _resolve_or_create_bom_item(
    db: DatabaseClient,
    assembly_id: UUID,
    part_id: UUID,
    row: NormalizedRow,
    debug: bool = False
) -> UUID:
    """
    Resolve an existing bom_item or create a new one.
    
    This implements the USAGE IDENTITY resolution:
    - A bom_item represents "This part used in this assembly in this role"
    - Match using: assembly_id, part_id, context similarity (notes, placement, etc.)
    - Reference designator is NOT used for matching (it's snapshot-specific)
    - If matched → reuse
    - Else → create new
    
    CRITICAL: Reference designator is NOT part of bom_items.context.
    This ensures refdes changes (D1-D8 → D1-D6) show as MODIFY, not remove+add.
    
    This is what allows stable diffs across snapshots.
    
    Args:
        db: Database client
        assembly_id: Assembly ID
        part_id: Part ID
        row: Normalized row
        debug: Enable debug logging
        
    Returns:
        BOM item UUID (existing or newly created)
    """
    context = _extract_bom_item_context(row)
    
    # Find similar bom_items
    # NOTE: Context no longer includes reference_designator, so matching
    # is based on stable usage context (notes, placement, torque, etc.)
    similar_items = db.find_similar_bom_items(
        assembly_id=assembly_id,
        part_id=part_id,
        context=context,
        similarity_threshold=0.7  # Lower threshold for usage context matching
    )
    
    if similar_items:
        # Use the best match
        best_match_id, confidence = similar_items[0]
        
        if debug:
            logger.info(
                f"BOM item match: part {part_id} in assembly {assembly_id} "
                f"→ existing bom_item {best_match_id} (confidence: {confidence:.2f})"
            )
        
        return best_match_id
    
    # No good match found, create new bom_item
    bom_item_id = db.create_bom_item(
        assembly_id=assembly_id,
        part_id=part_id,
        context=context
    )
    
    if debug:
        logger.info(
            f"BOM item created: part {part_id} in assembly {assembly_id} "
            f"→ new bom_item {bom_item_id}"
        )
    
    return bom_item_id


def ingest_bom_snapshot(
    org_id: UUID,
    rows: List[NormalizedRow],
    db: DatabaseClient,
    assembly_id: Optional[UUID] = None,
    assembly_name: Optional[str] = None,
    parent_snapshot_id: Optional[UUID] = None,
    debug: bool = False
) -> UUID:
    """
    Ingest a BOM snapshot into the database.
    
    This function implements the complete snapshot-based ingestion flow:
    
    1. Resolve assembly (by ID or name)
    2. For each row:
       a. Resolve or create part (design intent)
       b. Resolve or create bom_item (usage identity)
    3. Create snapshot (always, even if nothing changed)
    4. Insert snapshot_items (materialized state)
    
    IMPORTANT: You must specify EITHER assembly_id OR assembly_name:
    
    - Use assembly_id when updating an existing assembly (new version/snapshot)
      Example: BOM v2.0 is an updated version of BOM v1.0 (same assembly)
    
    - Use assembly_name when creating a new assembly
      Example: A completely new product/board (different assembly)
    
    The function is idempotent per snapshot: calling it multiple times with the
    same data should produce the same result (reusing entities, creating one snapshot).
    
    Args:
        org_id: Organization ID
        rows: List of normalized rows from bomkit
        db: Database client instance
        assembly_id: UUID of existing assembly (for updating existing BOM)
                    Mutually exclusive with assembly_name
        assembly_name: Name of new assembly to create (for creating new BOM)
                      Mutually exclusive with assembly_id
        parent_snapshot_id: Optional parent snapshot for lineage tracking
        debug: Enable debug logging of identity resolution decisions
        
    Returns:
        UUID of the newly created snapshot
        
    Raises:
        ValueError: If neither or both assembly_id and assembly_name are provided
        Exception: If database operations fail (transaction will be rolled back)
        
    Examples:
        # Updating an existing assembly (new snapshot/version)
        snapshot_id = ingest_bom_snapshot(
            org_id=org_uuid,
            assembly_id=existing_assembly_uuid,  # Explicit: updating this assembly
            rows=normalized_rows,
            db=db
        )
        
        # Creating a new assembly
        snapshot_id = ingest_bom_snapshot(
            org_id=org_uuid,
            assembly_name="Main Board v2.0",  # Explicit: new assembly
            rows=normalized_rows,
            db=db
        )
    """
    if not rows:
        raise ValueError("Cannot ingest empty BOM snapshot")
    
    # Validate that exactly one of assembly_id or assembly_name is provided
    if assembly_id is None and assembly_name is None:
        raise ValueError(
            "Must provide either 'assembly_id' (to update existing assembly) "
            "or 'assembly_name' (to create new assembly), but not both."
        )
    
    if assembly_id is not None and assembly_name is not None:
        raise ValueError(
            "Cannot provide both 'assembly_id' and 'assembly_name'. "
            "Use 'assembly_id' to update an existing assembly, "
            "or 'assembly_name' to create a new assembly."
        )
    
    # Begin transaction for atomicity
    db.begin_transaction()
    
    try:
        # ========================================================================
        # STEP 0: Ensure Organization Exists
        # ========================================================================
        # Organizations must exist before creating assemblies (foreign key constraint)
        # Create organization if it doesn't exist
        org_id = db.get_or_create_organization(org_id=org_id)
        
        if debug:
            logger.info(f"Organization resolved: {org_id}")
        
        # ========================================================================
        # STEP 1: Resolve Assembly
        # ========================================================================
        # Assembly represents the BOM context (product / board / assembly)
        # 
        # If assembly_id provided: Use existing assembly (updating/versioning)
        # If assembly_name provided: Create or find assembly by name (new assembly)
        
        if assembly_id is not None:
            # Explicit: User wants to update this specific assembly
            # Verify the assembly exists and belongs to the org
            assembly_id = db.get_assembly_by_id(org_id=org_id, assembly_id=assembly_id)
            if debug:
                logger.info(f"Using existing assembly: {assembly_id} (explicit update)")
        else:
            # Create or find assembly by name (new assembly or first snapshot)
            assembly_id = db.get_or_create_assembly(
                org_id=org_id,
                assembly_name=assembly_name
            )
            if debug:
                logger.info(f"Assembly resolved: '{assembly_name}' → {assembly_id} (new or found by name)")
        
        # ========================================================================
        # STEP 2: Resolve or Create Parts and BOM Items
        # ========================================================================
        # We need to resolve parts and bom_items before creating the snapshot
        # because snapshot_items reference bom_item_id
        
        bom_item_mappings = []  # List of (bom_item_id, row) tuples
        
        for row in rows:
            # --------------------------------------------------------------------
            # STEP 2a: Resolve or Create Part (DESIGN INTENT)
            # --------------------------------------------------------------------
            # Parts represent design intent (abstract, reusable)
            # We match using name similarity and overlapping attributes
            # DO NOT create a new part per row - reuse across uploads
            part_id = _resolve_or_create_part(
                db=db,
                org_id=org_id,
                row=row,
                debug=debug
            )
            
            # --------------------------------------------------------------------
            # STEP 2b: Resolve or Create BOM Item (USAGE IDENTITY)
            # --------------------------------------------------------------------
            # BOM items represent usage of a part in an assembly
            # Match using: assembly_id, part_id, context similarity
            # This is what allows stable diffs across snapshots
            bom_item_id = _resolve_or_create_bom_item(
                db=db,
                assembly_id=assembly_id,
                part_id=part_id,
                row=row,
                debug=debug
            )
            
            bom_item_mappings.append((bom_item_id, row))
        
        # ========================================================================
        # STEP 3: Create Snapshot (ALWAYS)
        # ========================================================================
        # Every ingestion creates a snapshot, even if nothing changed
        # Snapshots are immutable - we never update them
        snapshot_id = db.create_snapshot(
            org_id=org_id,
            assembly_id=assembly_id,
            source="csv",
            parent_snapshot_id=parent_snapshot_id
        )
        
        if debug:
            logger.info(f"Snapshot created: {snapshot_id} (parent: {parent_snapshot_id})")
        
        # ========================================================================
        # STEP 4: Insert Snapshot Items
        # ========================================================================
        # For each resolved bom_item, insert snapshot state
        # This materializes the BOM state at this point in time
        #
        # CRITICAL: Multiple rows may resolve to the same bom_item_id
        # (same part, same assembly, same context, but different CSV rows).
        # We aggregate by bom_item_id to avoid duplicate key violations.
        
        # Aggregate by bom_item_id
        # If the same bom_item appears multiple times, we need to decide:
        # - Option 1: Sum quantities (if quantities are per-instance)
        # - Option 2: Keep first occurrence (if rows are duplicates)
        # - Option 3: Use ON CONFLICT to handle duplicates
        #
        # For now, we'll aggregate quantities and merge attributes
        bom_item_aggregated = {}  # bom_item_id -> (total_quantity, merged_attributes, row_indices)
        
        for bom_item_id, row in bom_item_mappings:
            if bom_item_id not in bom_item_aggregated:
                bom_item_aggregated[bom_item_id] = {
                    'quantity': row.quantity or 0,
                    'attributes': _extract_snapshot_attributes(row),
                    'row_indices': [row.row_index]
                }
            else:
                # Same bom_item_id appears again - aggregate
                existing = bom_item_aggregated[bom_item_id]
                # Sum quantities (if both are numeric)
                if row.quantity is not None:
                    existing['quantity'] = (existing['quantity'] or 0) + row.quantity
                # Merge attributes (prefer non-empty values, combine row_indices)
                existing_attrs = existing['attributes']
                new_attrs = _extract_snapshot_attributes(row)
                # Merge: keep all unique keys, combine row_indices
                for key, value in new_attrs.items():
                    if key == 'row_index':
                        # Combine row indices
                        if 'row_indices' not in existing_attrs:
                            existing_attrs['row_indices'] = existing['row_indices'].copy()
                        if row.row_index not in existing_attrs['row_indices']:
                            existing_attrs['row_indices'].append(row.row_index)
                    elif key not in existing_attrs or not existing_attrs[key]:
                        # Use new value if existing is empty
                        existing_attrs[key] = value
                    elif key == 'reference_designator':
                        # For refdes, we might want to combine (e.g., "D1-D6, D7-D8")
                        # But for now, keep the first one to avoid complexity
                        pass
                existing['row_indices'].append(row.row_index)
        
        created_count = 0
        
        for bom_item_id, aggregated in bom_item_aggregated.items():
            # Extract aggregated data
            quantity = aggregated['quantity'] if aggregated['quantity'] > 0 else None
            snapshot_attributes = aggregated['attributes']
            
            # Compute deterministic checksum
            checksum = _compute_checksum(
                quantity=quantity,
                attributes=snapshot_attributes
            )
            
            # Insert snapshot_item (idempotent - uses ON CONFLICT)
            db.insert_snapshot_item(
                snapshot_id=snapshot_id,
                bom_item_id=bom_item_id,
                quantity=quantity,
                attributes=snapshot_attributes,
                checksum=checksum
            )
            
            created_count += 1
        
        if debug:
            logger.info(
                f"Snapshot items inserted: {created_count} items "
                f"(reused entities: {len(set(bom_item_id for bom_item_id, _ in bom_item_mappings))} bom_items)"
            )
        
        # Commit transaction
        db.commit_transaction()
        
        if debug:
            logger.info(f"Ingestion complete: snapshot {snapshot_id}")
        
        return snapshot_id
        
    except Exception as e:
        # Rollback on any error
        db.rollback_transaction()
        logger.error(f"BOM snapshot ingestion failed: {e}", exc_info=True)
        raise

