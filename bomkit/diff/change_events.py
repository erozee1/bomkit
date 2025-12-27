"""
Change Event Classification for BOM Diffs (Layer 2)

This module implements a deterministic classifier that transforms semantic diffs
into typed, actionable change events.

ARCHITECTURE:
- Layer 1 (snapshot_diff.py): Answers "What changed?"
- Layer 2 (this module): Answers "What kind of engineering change is this?"
- Layer 3 (future, LLM-powered): Answers "What does this mean?"

DESIGN PRINCIPLES:
1. Deterministic: Same inputs always produce same outputs
2. Explainable: Every event carries evidence (field-level diffs)
3. Auditable: Engineers can ask "why was this classified this way?"
4. Conservative: Prefer under-classification to false positives

This layer is intentionally NOT:
- Reasoning about downstream impact
- Inferring meaning or intent
- Using LLMs or heuristics
- Being "clever"

It behaves more like a compiler than an AI.
"""

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Dict, List, Optional, Set, Any, Tuple
from uuid import UUID

from .snapshot_diff import DiffResult, ModifiedItem, FieldChange


# =============================================================================
# CHANGE EVENT TAXONOMY (v1)
# =============================================================================
# A minimal, high-confidence set of event types.
# Intentionally small to avoid over-classification.

class ChangeEventType(Enum):
    """
    Canonical change event types.
    
    These represent typed engineering change intents that engineers and
    automated systems can rely on.
    
    IMPORTANT: This taxonomy is intentionally minimal.
    - Each type represents a distinct, actionable change intent
    - Types are mutually exclusive at the primary level
    - When in doubt, use UNCLASSIFIED_CHANGE
    """
    # Structural changes (item existence)
    PART_ADDED = auto()            # Item exists in B but not A
    PART_REMOVED = auto()          # Item exists in A but not B
    
    # Identity/source changes
    PART_SUBSTITUTED = auto()      # Different part source (manufacturer + MPN changed)
    MANUFACTURER_CHANGED = auto()  # Same MPN, different manufacturer
    
    # Quantity changes
    QUANTITY_CHANGED = auto()      # Quantity increased or decreased
    
    # Specification changes
    SPEC_ATTRIBUTE_CHANGED = auto()  # Value, tolerance, package, etc.
    
    # Placement changes
    REFERENCE_DESIGNATOR_CHANGED = auto()  # Reference designator(s) changed
    
    # Fallback (low confidence)
    UNCLASSIFIED_CHANGE = auto()   # Changed but cannot confidently classify


class Severity(Enum):
    """
    Change severity levels.
    
    Severity indicates the potential impact of the change,
    NOT the urgency or priority of review.
    
    These are conservative estimates - actual impact depends on context.
    """
    HIGH = auto()    # Structural change, new sourcing required, etc.
    MEDIUM = auto()  # May affect procurement, manufacturing, or specs
    LOW = auto()     # Minor change, likely no action needed


class Domain(Enum):
    """
    Affected engineering/business domains.
    
    These indicate which teams or workflows might be affected by the change.
    Used for routing and filtering, not for classification.
    """
    ENGINEERING = auto()    # Electrical, mechanical, or design engineering
    PROCUREMENT = auto()    # Sourcing, purchasing, vendor management
    MANUFACTURING = auto()  # Assembly, production, process engineering
    QUALITY = auto()        # Compliance, testing, reliability


# =============================================================================
# ITEM DELTA (Canonical Abstraction)
# =============================================================================
# Reduces raw field-level diffs into a stable semantic surface.
# This is the intermediate representation between Layer 1 and classification.

@dataclass
class ItemDelta:
    """
    Canonical representation of what changed for a single BOM item.
    
    This abstraction:
    - Reduces raw field-level diffs into boolean flags and sets
    - Provides a stable semantic surface for classification rules
    - Preserves evidence for audit and explanation
    - Is computed once and used by all classifiers
    
    ItemDelta represents FACTS, not DECISIONS.
    Classification rules interpret these facts into change events.
    """
    bom_item_id: UUID
    part_id: Optional[UUID] = None
    
    # Existence flags (mutually exclusive)
    added: bool = False           # Item only exists in snapshot B
    removed: bool = False         # Item only exists in snapshot A
    
    # Field change flags (computed from FieldChange list)
    quantity_changed: bool = False
    manufacturer_changed: bool = False
    mpn_changed: bool = False
    reference_designator_changed: bool = False
    
    # Set of changed attribute names (for SPEC_ATTRIBUTE_CHANGED detection)
    changed_attributes: Set[str] = field(default_factory=set)
    
    # Evidence: raw field-level diffs that produced this delta
    # Preserved for audit, explanation, and UI rendering
    field_changes: List[FieldChange] = field(default_factory=list)
    
    # Quantity delta details (for downstream use)
    quantity_from: Optional[float] = None
    quantity_to: Optional[float] = None
    
    def has_any_change(self) -> bool:
        """Returns True if any change was detected."""
        return (
            self.added or
            self.removed or
            self.quantity_changed or
            self.manufacturer_changed or
            self.mpn_changed or
            self.reference_designator_changed or
            len(self.changed_attributes) > 0
        )


# =============================================================================
# CHANGE EVENT (Output Type)
# =============================================================================
# The final product of classification - what engineers see and act on.

@dataclass
class ChangeEvent:
    """
    A typed, actionable change event.
    
    This is the output of Layer 2 classification.
    
    Each event:
    - Has a single primary type (mutually exclusive)
    - Has severity (impact estimate)
    - Has affected domains (routing hint)
    - Carries evidence (field-level diffs for explanation)
    
    Events are designed to be:
    - Glanceable (engineer sees event type, knows what happened)
    - Actionable (type maps to workflows)
    - Auditable (evidence explains classification)
    """
    # Identity
    bom_item_id: UUID
    part_id: Optional[UUID]
    
    # Classification
    event_type: ChangeEventType
    severity: Severity
    affected_domains: List[Domain]
    
    # Evidence (non-negotiable)
    evidence: List[FieldChange]
    
    # Human-readable summary (generated, not computed)
    summary: str = ""
    
    # Optional: Item delta that produced this event (for debugging)
    delta: Optional[ItemDelta] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary for JSON output."""
        return {
            "bom_item_id": str(self.bom_item_id),
            "part_id": str(self.part_id) if self.part_id else None,
            "event_type": self.event_type.name,
            "severity": self.severity.name,
            "affected_domains": [d.name for d in self.affected_domains],
            "evidence": [
                {
                    "type": e.type,
                    "field": e.field,
                    "from_value": e.from_value,
                    "to_value": e.to_value
                }
                for e in self.evidence
            ],
            "summary": self.summary
        }


# =============================================================================
# CLASSIFICATION RESULT
# =============================================================================

@dataclass
class ClassificationResult:
    """
    Complete result of change event classification.
    
    Contains:
    - All classified change events
    - Summary statistics
    - Original diff reference for context
    """
    snapshot_a_id: UUID
    snapshot_b_id: UUID
    
    # Classified events (the main output)
    events: List[ChangeEvent]
    
    # Statistics
    total_changes: int
    added_count: int
    removed_count: int
    modified_count: int
    
    # Events by type (for quick filtering)
    def events_by_type(self, event_type: ChangeEventType) -> List[ChangeEvent]:
        """Filter events by type."""
        return [e for e in self.events if e.event_type == event_type]
    
    def events_by_severity(self, severity: Severity) -> List[ChangeEvent]:
        """Filter events by severity."""
        return [e for e in self.events if e.severity == severity]
    
    def events_by_domain(self, domain: Domain) -> List[ChangeEvent]:
        """Filter events by affected domain."""
        return [e for e in self.events if domain in e.affected_domains]
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary for JSON output."""
        return {
            "snapshot_a_id": str(self.snapshot_a_id),
            "snapshot_b_id": str(self.snapshot_b_id),
            "total_changes": self.total_changes,
            "added_count": self.added_count,
            "removed_count": self.removed_count,
            "modified_count": self.modified_count,
            "events": [e.to_dict() for e in self.events]
        }


# =============================================================================
# DELTA COMPUTATION
# =============================================================================
# Transform raw diffs into canonical ItemDelta objects.

# Semantic attribute keys that represent spec changes
SPEC_ATTRIBUTE_KEYS = {
    "value",
    "tolerance",
    "package",
    "footprint",
    "material",
    "voltage_rating",
    "current_rating",
    "power_rating",
    "temperature_rating",
    "description",
    "unit",
}

# Manufacturer-related attribute keys
MANUFACTURER_KEYS = {
    "manufacturer",
    "mfr",
    "vendor",
    "brand",
}

# MPN-related attribute keys
MPN_KEYS = {
    "manufacturer_part_number",
    "mpn",
    "mfr_part_number",
    "part_number",
    "vendor_part_number",
}

# Reference designator attribute keys
REFDES_KEYS = {
    "reference_designator",
    "refdes",
    "designator",
}


def _compute_item_delta_from_modified(modified: ModifiedItem) -> ItemDelta:
    """
    Compute an ItemDelta from a ModifiedItem.
    
    This function analyzes field-level changes and sets the appropriate
    flags and attribute sets on the ItemDelta.
    
    Args:
        modified: ModifiedItem from Layer 1 diff
        
    Returns:
        ItemDelta with all flags computed
    """
    delta = ItemDelta(
        bom_item_id=modified.bom_item_id,
        field_changes=modified.changes
    )
    
    for change in modified.changes:
        # Quantity change
        if change.type == "QUANTITY_CHANGED":
            delta.quantity_changed = True
            delta.quantity_from = change.from_value
            delta.quantity_to = change.to_value
        
        # Attribute changes
        elif change.type in ("ATTRIBUTE_CHANGED", "ATTRIBUTE_ADDED", "ATTRIBUTE_REMOVED"):
            field_name = change.field
            if field_name:
                field_lower = field_name.lower()
                
                # Check for MPN change FIRST (before manufacturer, since "manufacturer_part_number" contains "manufacturer")
                if field_lower in MPN_KEYS or any(k in field_lower for k in MPN_KEYS):
                    delta.mpn_changed = True
                
                # Check for manufacturer change (after MPN to avoid false matches)
                elif field_lower in MANUFACTURER_KEYS or any(k in field_lower for k in MANUFACTURER_KEYS):
                    delta.manufacturer_changed = True
                
                # Check for reference designator change
                elif field_lower in REFDES_KEYS or any(k in field_lower for k in REFDES_KEYS):
                    delta.reference_designator_changed = True
                
                # Check for spec attribute change
                elif field_lower in SPEC_ATTRIBUTE_KEYS or any(k in field_lower for k in SPEC_ATTRIBUTE_KEYS):
                    delta.changed_attributes.add(field_name)
                
                # Unknown attribute - still track it
                else:
                    delta.changed_attributes.add(field_name)
    
    return delta


def _compute_item_delta_added(bom_item_id: UUID) -> ItemDelta:
    """
    Compute an ItemDelta for an added item.
    
    Args:
        bom_item_id: UUID of the added item
        
    Returns:
        ItemDelta with added=True
    """
    return ItemDelta(
        bom_item_id=bom_item_id,
        added=True
    )


def _compute_item_delta_removed(bom_item_id: UUID) -> ItemDelta:
    """
    Compute an ItemDelta for a removed item.
    
    Args:
        bom_item_id: UUID of the removed item
        
    Returns:
        ItemDelta with removed=True
    """
    return ItemDelta(
        bom_item_id=bom_item_id,
        removed=True
    )


# =============================================================================
# CLASSIFICATION RULES
# =============================================================================
# Ordered, explicit rules. First match wins.
# Each rule is a function: (ItemDelta) -> Optional[ChangeEvent]

def _classify_added(delta: ItemDelta) -> Optional[ChangeEvent]:
    """
    Rule: Item only exists in snapshot B.
    
    Produces: PART_ADDED (HIGH severity)
    """
    if not delta.added:
        return None
    
    return ChangeEvent(
        bom_item_id=delta.bom_item_id,
        part_id=delta.part_id,
        event_type=ChangeEventType.PART_ADDED,
        severity=Severity.HIGH,
        affected_domains=[Domain.ENGINEERING, Domain.PROCUREMENT],
        evidence=delta.field_changes,
        summary="New part added to BOM",
        delta=delta
    )


def _classify_removed(delta: ItemDelta) -> Optional[ChangeEvent]:
    """
    Rule: Item only exists in snapshot A.
    
    Produces: PART_REMOVED (HIGH severity)
    """
    if not delta.removed:
        return None
    
    return ChangeEvent(
        bom_item_id=delta.bom_item_id,
        part_id=delta.part_id,
        event_type=ChangeEventType.PART_REMOVED,
        severity=Severity.HIGH,
        affected_domains=[Domain.ENGINEERING, Domain.PROCUREMENT],
        evidence=delta.field_changes,
        summary="Part removed from BOM",
        delta=delta
    )


def _classify_substituted(delta: ItemDelta) -> Optional[ChangeEvent]:
    """
    Rule: Both manufacturer AND MPN changed.
    
    This indicates a complete part substitution - sourcing from a different
    vendor with a different part number.
    
    Produces: PART_SUBSTITUTED (HIGH severity)
    """
    if not (delta.manufacturer_changed and delta.mpn_changed):
        return None
    
    return ChangeEvent(
        bom_item_id=delta.bom_item_id,
        part_id=delta.part_id,
        event_type=ChangeEventType.PART_SUBSTITUTED,
        severity=Severity.HIGH,
        affected_domains=[Domain.PROCUREMENT, Domain.ENGINEERING],
        evidence=delta.field_changes,
        summary="Part substituted (different manufacturer and MPN)",
        delta=delta
    )


def _classify_manufacturer_changed(delta: ItemDelta) -> Optional[ChangeEvent]:
    """
    Rule: Manufacturer changed (without MPN change).
    
    This could indicate:
    - Same part, different authorized distributor
    - Second-source qualification
    - Vendor change for same spec part
    
    Produces: MANUFACTURER_CHANGED (MEDIUM severity)
    """
    if not delta.manufacturer_changed:
        return None
    
    # If MPN also changed, this is a substitution (handled above)
    if delta.mpn_changed:
        return None
    
    return ChangeEvent(
        bom_item_id=delta.bom_item_id,
        part_id=delta.part_id,
        event_type=ChangeEventType.MANUFACTURER_CHANGED,
        severity=Severity.MEDIUM,
        affected_domains=[Domain.PROCUREMENT],
        evidence=delta.field_changes,
        summary="Manufacturer changed",
        delta=delta
    )


def _classify_quantity_changed(delta: ItemDelta) -> Optional[ChangeEvent]:
    """
    Rule: Quantity changed.
    
    This affects procurement (ordering) and manufacturing (kitting).
    
    Produces: QUANTITY_CHANGED (MEDIUM severity)
    """
    if not delta.quantity_changed:
        return None
    
    # Build summary with delta details
    if delta.quantity_from is not None and delta.quantity_to is not None:
        qty_from = int(delta.quantity_from) if delta.quantity_from == int(delta.quantity_from) else delta.quantity_from
        qty_to = int(delta.quantity_to) if delta.quantity_to == int(delta.quantity_to) else delta.quantity_to
        summary = f"Quantity changed: {qty_from} → {qty_to}"
    else:
        summary = "Quantity changed"
    
    return ChangeEvent(
        bom_item_id=delta.bom_item_id,
        part_id=delta.part_id,
        event_type=ChangeEventType.QUANTITY_CHANGED,
        severity=Severity.MEDIUM,
        affected_domains=[Domain.PROCUREMENT, Domain.MANUFACTURING],
        evidence=delta.field_changes,
        summary=summary,
        delta=delta
    )


def _classify_refdes_changed(delta: ItemDelta) -> Optional[ChangeEvent]:
    """
    Rule: Reference designator changed.
    
    This affects manufacturing (placement, silkscreen) but typically
    does not affect procurement or electrical design.
    
    Produces: REFERENCE_DESIGNATOR_CHANGED (LOW severity)
    """
    if not delta.reference_designator_changed:
        return None
    
    return ChangeEvent(
        bom_item_id=delta.bom_item_id,
        part_id=delta.part_id,
        event_type=ChangeEventType.REFERENCE_DESIGNATOR_CHANGED,
        severity=Severity.LOW,
        affected_domains=[Domain.MANUFACTURING],
        evidence=delta.field_changes,
        summary="Reference designator changed",
        delta=delta
    )


def _classify_spec_attribute_changed(delta: ItemDelta) -> Optional[ChangeEvent]:
    """
    Rule: Specification attributes changed.
    
    This includes value, tolerance, package, material, ratings, etc.
    These changes may affect engineering validation.
    
    Produces: SPEC_ATTRIBUTE_CHANGED (MEDIUM severity)
    """
    if not delta.changed_attributes:
        return None
    
    # Filter to only spec-like attributes
    spec_changes = delta.changed_attributes & SPEC_ATTRIBUTE_KEYS
    
    if not spec_changes:
        # Changed attributes are not recognized spec attributes
        # Fall through to UNCLASSIFIED
        return None
    
    # Build summary listing changed attributes
    attrs_list = ", ".join(sorted(spec_changes))
    summary = f"Specification changed: {attrs_list}"
    
    return ChangeEvent(
        bom_item_id=delta.bom_item_id,
        part_id=delta.part_id,
        event_type=ChangeEventType.SPEC_ATTRIBUTE_CHANGED,
        severity=Severity.MEDIUM,
        affected_domains=[Domain.ENGINEERING],
        evidence=delta.field_changes,
        summary=summary,
        delta=delta
    )


def _classify_unclassified(delta: ItemDelta) -> Optional[ChangeEvent]:
    """
    Fallback rule: Changed but cannot confidently classify.
    
    This is the safety net - if we get here, something changed
    but we don't have a high-confidence classification.
    
    Produces: UNCLASSIFIED_CHANGE (LOW severity)
    """
    if not delta.has_any_change():
        return None
    
    # Build summary describing what changed
    changes_desc = []
    if delta.changed_attributes:
        changes_desc.append(f"attributes: {', '.join(sorted(delta.changed_attributes))}")
    if delta.mpn_changed:
        changes_desc.append("MPN")
    
    if changes_desc:
        summary = f"Unclassified change: {'; '.join(changes_desc)}"
    else:
        summary = "Unclassified change"
    
    return ChangeEvent(
        bom_item_id=delta.bom_item_id,
        part_id=delta.part_id,
        event_type=ChangeEventType.UNCLASSIFIED_CHANGE,
        severity=Severity.LOW,
        affected_domains=[],  # Unknown domains
        evidence=delta.field_changes,
        summary=summary,
        delta=delta
    )


# Ordered list of classification rules
# CRITICAL: Order matters - first match wins
CLASSIFICATION_RULES = [
    _classify_added,
    _classify_removed,
    _classify_substituted,
    _classify_manufacturer_changed,
    _classify_quantity_changed,
    _classify_refdes_changed,
    _classify_spec_attribute_changed,
    _classify_unclassified,  # Fallback - always matches if has_any_change()
]


def _classify_delta(delta: ItemDelta) -> Optional[ChangeEvent]:
    """
    Classify a single ItemDelta into a ChangeEvent.
    
    Applies classification rules in order until one matches.
    First match wins.
    
    Args:
        delta: The ItemDelta to classify
        
    Returns:
        ChangeEvent if classification succeeds, None if no change
    """
    if not delta.has_any_change():
        return None
    
    for rule in CLASSIFICATION_RULES:
        event = rule(delta)
        if event is not None:
            return event
    
    # Should never reach here if rules are complete
    # (UNCLASSIFIED always matches if has_any_change())
    return None


# =============================================================================
# MAIN CLASSIFICATION FUNCTION
# =============================================================================

def classify_diff(diff_result: DiffResult) -> ClassificationResult:
    """
    Classify a DiffResult into typed ChangeEvents.
    
    This is the main entry point for Layer 2 classification.
    
    The function:
    1. Computes ItemDeltas from raw diffs (canonical abstraction)
    2. Applies ordered classification rules to each delta
    3. Produces a list of typed, actionable ChangeEvents
    
    Each change event:
    - Has a single primary type
    - Has severity and affected domains
    - Carries evidence (field-level diffs)
    
    Args:
        diff_result: The DiffResult from Layer 1 diff engine
        
    Returns:
        ClassificationResult with all classified events
        
    Example:
        >>> from bomkit.diff import diff_snapshots, classify_diff
        >>> diff = diff_snapshots(snapshot_a_id, snapshot_b_id, db)
        >>> result = classify_diff(diff)
        >>> for event in result.events:
        ...     print(f"{event.event_type.name}: {event.summary}")
    """
    events: List[ChangeEvent] = []
    
    # Process added items
    for bom_item_id in diff_result.added_items:
        delta = _compute_item_delta_added(bom_item_id)
        event = _classify_delta(delta)
        if event:
            events.append(event)
    
    # Process removed items
    for bom_item_id in diff_result.removed_items:
        delta = _compute_item_delta_removed(bom_item_id)
        event = _classify_delta(delta)
        if event:
            events.append(event)
    
    # Process modified items
    for modified in diff_result.modified_items:
        delta = _compute_item_delta_from_modified(modified)
        event = _classify_delta(delta)
        if event:
            events.append(event)
    
    return ClassificationResult(
        snapshot_a_id=diff_result.snapshot_a_id,
        snapshot_b_id=diff_result.snapshot_b_id,
        events=events,
        total_changes=len(events),
        added_count=len(diff_result.added_items),
        removed_count=len(diff_result.removed_items),
        modified_count=len(diff_result.modified_items)
    )


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def classify_and_summarize(diff_result: DiffResult) -> Dict[str, Any]:
    """
    Classify a diff and produce a summary dictionary.
    
    This is a convenience function for quick inspection and debugging.
    
    Args:
        diff_result: The DiffResult from Layer 1 diff engine
        
    Returns:
        Dictionary with classification summary
    """
    result = classify_diff(diff_result)
    
    # Count events by type
    by_type = {}
    for event in result.events:
        type_name = event.event_type.name
        by_type[type_name] = by_type.get(type_name, 0) + 1
    
    # Count events by severity
    by_severity = {}
    for event in result.events:
        sev_name = event.severity.name
        by_severity[sev_name] = by_severity.get(sev_name, 0) + 1
    
    return {
        "snapshot_a_id": str(result.snapshot_a_id),
        "snapshot_b_id": str(result.snapshot_b_id),
        "total_events": result.total_changes,
        "events_by_type": by_type,
        "events_by_severity": by_severity,
        "high_severity_count": by_severity.get("HIGH", 0),
        "events": [e.to_dict() for e in result.events]
    }


def get_high_priority_events(diff_result: DiffResult) -> List[ChangeEvent]:
    """
    Get only HIGH severity events from a diff.
    
    This is useful for alerts and notifications.
    
    Args:
        diff_result: The DiffResult from Layer 1 diff engine
        
    Returns:
        List of HIGH severity ChangeEvents
    """
    result = classify_diff(diff_result)
    return result.events_by_severity(Severity.HIGH)


def get_procurement_events(diff_result: DiffResult) -> List[ChangeEvent]:
    """
    Get events that affect the PROCUREMENT domain.
    
    This is useful for routing changes to procurement teams.
    
    Args:
        diff_result: The DiffResult from Layer 1 diff engine
        
    Returns:
        List of ChangeEvents affecting PROCUREMENT
    """
    result = classify_diff(diff_result)
    return result.events_by_domain(Domain.PROCUREMENT)

