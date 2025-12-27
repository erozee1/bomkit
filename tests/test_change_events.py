"""
Unit tests for Change Event Classification (Layer 2).

These tests verify that:
1. Classification is deterministic (same inputs → same outputs)
2. Rules are applied in correct order (first-match wins)
3. Evidence is preserved on all events
4. Fallback to UNCLASSIFIED works correctly
"""

import pytest
from uuid import uuid4, UUID

# Import directly from modules to avoid circular import issues
from bomkit.diff.snapshot_diff import (
    DiffResult,
    ModifiedItem,
    FieldChange,
)
from bomkit.diff.change_events import (
    classify_diff,
    classify_and_summarize,
    get_high_priority_events,
    get_procurement_events,
    ChangeEventType,
    Severity,
    Domain,
    ItemDelta,
    ChangeEvent,
    ClassificationResult,
    _compute_item_delta_from_modified,
    _compute_item_delta_added,
    _compute_item_delta_removed,
)


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture
def snapshot_a_id():
    return uuid4()


@pytest.fixture
def snapshot_b_id():
    return uuid4()


def make_diff_result(
    snapshot_a_id: UUID,
    snapshot_b_id: UUID,
    added_items: list = None,
    removed_items: list = None,
    modified_items: list = None,
    unchanged_count: int = 0
) -> DiffResult:
    """Helper to create DiffResult objects for testing."""
    return DiffResult(
        snapshot_a_id=snapshot_a_id,
        snapshot_b_id=snapshot_b_id,
        added_items=added_items or [],
        removed_items=removed_items or [],
        modified_items=modified_items or [],
        unchanged_count=unchanged_count
    )


def make_modified_item(
    bom_item_id: UUID = None,
    changes: list = None
) -> ModifiedItem:
    """Helper to create ModifiedItem objects for testing."""
    return ModifiedItem(
        bom_item_id=bom_item_id or uuid4(),
        changes=changes or []
    )


def make_field_change(
    change_type: str,
    field: str = None,
    from_value=None,
    to_value=None
) -> FieldChange:
    """Helper to create FieldChange objects for testing."""
    return FieldChange(
        type=change_type,
        field=field,
        from_value=from_value,
        to_value=to_value
    )


# =============================================================================
# DELTA COMPUTATION TESTS
# =============================================================================

class TestItemDeltaComputation:
    """Tests for computing ItemDelta from raw diffs."""
    
    def test_delta_added_item(self):
        """Added item should have added=True."""
        bom_id = uuid4()
        delta = _compute_item_delta_added(bom_id)
        
        assert delta.bom_item_id == bom_id
        assert delta.added is True
        assert delta.removed is False
        assert delta.has_any_change() is True
    
    def test_delta_removed_item(self):
        """Removed item should have removed=True."""
        bom_id = uuid4()
        delta = _compute_item_delta_removed(bom_id)
        
        assert delta.bom_item_id == bom_id
        assert delta.removed is True
        assert delta.added is False
        assert delta.has_any_change() is True
    
    def test_delta_quantity_changed(self):
        """Quantity change should set quantity_changed=True."""
        modified = make_modified_item(changes=[
            make_field_change("QUANTITY_CHANGED", from_value=5, to_value=10)
        ])
        
        delta = _compute_item_delta_from_modified(modified)
        
        assert delta.quantity_changed is True
        assert delta.quantity_from == 5
        assert delta.quantity_to == 10
        assert delta.has_any_change() is True
    
    def test_delta_manufacturer_changed(self):
        """Manufacturer attribute change should set manufacturer_changed=True."""
        modified = make_modified_item(changes=[
            make_field_change("ATTRIBUTE_CHANGED", field="manufacturer",
                            from_value="Murata", to_value="TDK")
        ])
        
        delta = _compute_item_delta_from_modified(modified)
        
        assert delta.manufacturer_changed is True
        assert delta.mpn_changed is False
        assert len(delta.field_changes) == 1
    
    def test_delta_mpn_changed(self):
        """MPN attribute change should set mpn_changed=True."""
        modified = make_modified_item(changes=[
            make_field_change("ATTRIBUTE_CHANGED", field="manufacturer_part_number",
                            from_value="ABC123", to_value="XYZ789")
        ])
        
        delta = _compute_item_delta_from_modified(modified)
        
        assert delta.mpn_changed is True
        assert delta.manufacturer_changed is False
    
    def test_delta_refdes_changed(self):
        """Reference designator change should set reference_designator_changed=True."""
        modified = make_modified_item(changes=[
            make_field_change("ATTRIBUTE_CHANGED", field="reference_designator",
                            from_value="R1,R2,R3", to_value="R1,R2")
        ])
        
        delta = _compute_item_delta_from_modified(modified)
        
        assert delta.reference_designator_changed is True
    
    def test_delta_spec_attribute_changed(self):
        """Spec attribute changes should populate changed_attributes set."""
        modified = make_modified_item(changes=[
            make_field_change("ATTRIBUTE_CHANGED", field="value",
                            from_value="10k", to_value="22k"),
            make_field_change("ATTRIBUTE_CHANGED", field="tolerance",
                            from_value="5%", to_value="1%")
        ])
        
        delta = _compute_item_delta_from_modified(modified)
        
        assert "value" in delta.changed_attributes
        assert "tolerance" in delta.changed_attributes
        assert len(delta.changed_attributes) == 2
    
    def test_delta_multiple_changes(self):
        """Multiple changes should all be detected."""
        modified = make_modified_item(changes=[
            make_field_change("QUANTITY_CHANGED", from_value=5, to_value=10),
            make_field_change("ATTRIBUTE_CHANGED", field="manufacturer",
                            from_value="Murata", to_value="TDK"),
            make_field_change("ATTRIBUTE_CHANGED", field="value",
                            from_value="10k", to_value="22k")
        ])
        
        delta = _compute_item_delta_from_modified(modified)
        
        assert delta.quantity_changed is True
        assert delta.manufacturer_changed is True
        assert "value" in delta.changed_attributes
        assert len(delta.field_changes) == 3
    
    def test_delta_no_change(self):
        """Empty changes should result in no change flags."""
        modified = make_modified_item(changes=[])
        delta = _compute_item_delta_from_modified(modified)
        
        assert delta.has_any_change() is False


# =============================================================================
# CLASSIFICATION RULE TESTS
# =============================================================================

class TestClassificationRules:
    """Tests for individual classification rules."""
    
    def test_classify_part_added(self, snapshot_a_id, snapshot_b_id):
        """Added items should classify as PART_ADDED."""
        bom_id = uuid4()
        diff = make_diff_result(
            snapshot_a_id, snapshot_b_id,
            added_items=[bom_id]
        )
        
        result = classify_diff(diff)
        
        assert len(result.events) == 1
        event = result.events[0]
        assert event.event_type == ChangeEventType.PART_ADDED
        assert event.severity == Severity.HIGH
        assert Domain.ENGINEERING in event.affected_domains
        assert Domain.PROCUREMENT in event.affected_domains
    
    def test_classify_part_removed(self, snapshot_a_id, snapshot_b_id):
        """Removed items should classify as PART_REMOVED."""
        bom_id = uuid4()
        diff = make_diff_result(
            snapshot_a_id, snapshot_b_id,
            removed_items=[bom_id]
        )
        
        result = classify_diff(diff)
        
        assert len(result.events) == 1
        event = result.events[0]
        assert event.event_type == ChangeEventType.PART_REMOVED
        assert event.severity == Severity.HIGH
    
    def test_classify_part_substituted(self, snapshot_a_id, snapshot_b_id):
        """Manufacturer + MPN change should classify as PART_SUBSTITUTED."""
        modified = make_modified_item(changes=[
            make_field_change("ATTRIBUTE_CHANGED", field="manufacturer",
                            from_value="Murata", to_value="TDK"),
            make_field_change("ATTRIBUTE_CHANGED", field="manufacturer_part_number",
                            from_value="ABC123", to_value="XYZ789")
        ])
        diff = make_diff_result(
            snapshot_a_id, snapshot_b_id,
            modified_items=[modified]
        )
        
        result = classify_diff(diff)
        
        assert len(result.events) == 1
        event = result.events[0]
        assert event.event_type == ChangeEventType.PART_SUBSTITUTED
        assert event.severity == Severity.HIGH
        assert Domain.PROCUREMENT in event.affected_domains
    
    def test_classify_manufacturer_changed_only(self, snapshot_a_id, snapshot_b_id):
        """Manufacturer change without MPN should classify as MANUFACTURER_CHANGED."""
        modified = make_modified_item(changes=[
            make_field_change("ATTRIBUTE_CHANGED", field="manufacturer",
                            from_value="Murata", to_value="TDK")
        ])
        diff = make_diff_result(
            snapshot_a_id, snapshot_b_id,
            modified_items=[modified]
        )
        
        result = classify_diff(diff)
        
        assert len(result.events) == 1
        event = result.events[0]
        assert event.event_type == ChangeEventType.MANUFACTURER_CHANGED
        assert event.severity == Severity.MEDIUM
    
    def test_classify_quantity_changed(self, snapshot_a_id, snapshot_b_id):
        """Quantity change should classify as QUANTITY_CHANGED."""
        modified = make_modified_item(changes=[
            make_field_change("QUANTITY_CHANGED", from_value=5, to_value=10)
        ])
        diff = make_diff_result(
            snapshot_a_id, snapshot_b_id,
            modified_items=[modified]
        )
        
        result = classify_diff(diff)
        
        assert len(result.events) == 1
        event = result.events[0]
        assert event.event_type == ChangeEventType.QUANTITY_CHANGED
        assert event.severity == Severity.MEDIUM
        assert Domain.PROCUREMENT in event.affected_domains
        assert Domain.MANUFACTURING in event.affected_domains
        assert "5 → 10" in event.summary
    
    def test_classify_refdes_changed(self, snapshot_a_id, snapshot_b_id):
        """Reference designator change should classify as REFERENCE_DESIGNATOR_CHANGED."""
        modified = make_modified_item(changes=[
            make_field_change("ATTRIBUTE_CHANGED", field="reference_designator",
                            from_value="R1,R2,R3", to_value="R1,R2")
        ])
        diff = make_diff_result(
            snapshot_a_id, snapshot_b_id,
            modified_items=[modified]
        )
        
        result = classify_diff(diff)
        
        assert len(result.events) == 1
        event = result.events[0]
        assert event.event_type == ChangeEventType.REFERENCE_DESIGNATOR_CHANGED
        assert event.severity == Severity.LOW
        assert Domain.MANUFACTURING in event.affected_domains
    
    def test_classify_spec_attribute_changed(self, snapshot_a_id, snapshot_b_id):
        """Spec attribute change should classify as SPEC_ATTRIBUTE_CHANGED."""
        modified = make_modified_item(changes=[
            make_field_change("ATTRIBUTE_CHANGED", field="value",
                            from_value="10k", to_value="22k")
        ])
        diff = make_diff_result(
            snapshot_a_id, snapshot_b_id,
            modified_items=[modified]
        )
        
        result = classify_diff(diff)
        
        assert len(result.events) == 1
        event = result.events[0]
        assert event.event_type == ChangeEventType.SPEC_ATTRIBUTE_CHANGED
        assert event.severity == Severity.MEDIUM
        assert Domain.ENGINEERING in event.affected_domains
        assert "value" in event.summary
    
    def test_classify_unclassified_unknown_attribute(self, snapshot_a_id, snapshot_b_id):
        """Unknown attribute change should classify as UNCLASSIFIED_CHANGE."""
        modified = make_modified_item(changes=[
            make_field_change("ATTRIBUTE_CHANGED", field="some_unknown_field",
                            from_value="foo", to_value="bar")
        ])
        diff = make_diff_result(
            snapshot_a_id, snapshot_b_id,
            modified_items=[modified]
        )
        
        result = classify_diff(diff)
        
        assert len(result.events) == 1
        event = result.events[0]
        assert event.event_type == ChangeEventType.UNCLASSIFIED_CHANGE
        assert event.severity == Severity.LOW


# =============================================================================
# RULE ORDERING TESTS
# =============================================================================

class TestRuleOrdering:
    """Tests that rules are applied in correct order (first-match wins)."""
    
    def test_substitution_beats_manufacturer_changed(self, snapshot_a_id, snapshot_b_id):
        """When both manufacturer and MPN change, PART_SUBSTITUTED wins."""
        modified = make_modified_item(changes=[
            make_field_change("ATTRIBUTE_CHANGED", field="manufacturer",
                            from_value="Murata", to_value="TDK"),
            make_field_change("ATTRIBUTE_CHANGED", field="manufacturer_part_number",
                            from_value="ABC123", to_value="XYZ789"),
            make_field_change("QUANTITY_CHANGED", from_value=5, to_value=10),
        ])
        diff = make_diff_result(
            snapshot_a_id, snapshot_b_id,
            modified_items=[modified]
        )
        
        result = classify_diff(diff)
        
        # Should be PART_SUBSTITUTED, not multiple events
        assert len(result.events) == 1
        assert result.events[0].event_type == ChangeEventType.PART_SUBSTITUTED
    
    def test_manufacturer_changed_before_spec_changed(self, snapshot_a_id, snapshot_b_id):
        """Manufacturer change takes precedence over spec attribute change."""
        modified = make_modified_item(changes=[
            make_field_change("ATTRIBUTE_CHANGED", field="manufacturer",
                            from_value="Murata", to_value="TDK"),
            make_field_change("ATTRIBUTE_CHANGED", field="value",
                            from_value="10k", to_value="22k")
        ])
        diff = make_diff_result(
            snapshot_a_id, snapshot_b_id,
            modified_items=[modified]
        )
        
        result = classify_diff(diff)
        
        assert len(result.events) == 1
        assert result.events[0].event_type == ChangeEventType.MANUFACTURER_CHANGED
    
    def test_quantity_before_refdes(self, snapshot_a_id, snapshot_b_id):
        """Quantity change takes precedence over reference designator change."""
        modified = make_modified_item(changes=[
            make_field_change("QUANTITY_CHANGED", from_value=5, to_value=10),
            make_field_change("ATTRIBUTE_CHANGED", field="reference_designator",
                            from_value="R1,R2", to_value="R1")
        ])
        diff = make_diff_result(
            snapshot_a_id, snapshot_b_id,
            modified_items=[modified]
        )
        
        result = classify_diff(diff)
        
        assert len(result.events) == 1
        assert result.events[0].event_type == ChangeEventType.QUANTITY_CHANGED


# =============================================================================
# EVIDENCE PRESERVATION TESTS
# =============================================================================

class TestEvidencePreservation:
    """Tests that evidence is preserved on all events."""
    
    def test_evidence_on_added(self, snapshot_a_id, snapshot_b_id):
        """Added events should have empty evidence (no field changes)."""
        bom_id = uuid4()
        diff = make_diff_result(
            snapshot_a_id, snapshot_b_id,
            added_items=[bom_id]
        )
        
        result = classify_diff(diff)
        event = result.events[0]
        
        # Added items have no field changes (they're new)
        assert event.evidence == []
        assert event.delta is not None
    
    def test_evidence_on_modified(self, snapshot_a_id, snapshot_b_id):
        """Modified events should preserve all field changes as evidence."""
        changes = [
            make_field_change("QUANTITY_CHANGED", from_value=5, to_value=10),
            make_field_change("ATTRIBUTE_CHANGED", field="value",
                            from_value="10k", to_value="22k")
        ]
        modified = make_modified_item(changes=changes)
        diff = make_diff_result(
            snapshot_a_id, snapshot_b_id,
            modified_items=[modified]
        )
        
        result = classify_diff(diff)
        event = result.events[0]
        
        # All field changes should be preserved
        assert len(event.evidence) == 2
        assert event.evidence[0].type == "QUANTITY_CHANGED"
        assert event.evidence[1].field == "value"
    
    def test_delta_attached_to_event(self, snapshot_a_id, snapshot_b_id):
        """ItemDelta should be attached to event for debugging."""
        modified = make_modified_item(changes=[
            make_field_change("QUANTITY_CHANGED", from_value=5, to_value=10)
        ])
        diff = make_diff_result(
            snapshot_a_id, snapshot_b_id,
            modified_items=[modified]
        )
        
        result = classify_diff(diff)
        event = result.events[0]
        
        assert event.delta is not None
        assert event.delta.quantity_changed is True
        assert event.delta.quantity_from == 5
        assert event.delta.quantity_to == 10


# =============================================================================
# INTEGRATION TESTS
# =============================================================================

class TestClassificationIntegration:
    """Integration tests for complete classification flows."""
    
    def test_empty_diff(self, snapshot_a_id, snapshot_b_id):
        """Empty diff should produce no events."""
        diff = make_diff_result(snapshot_a_id, snapshot_b_id, unchanged_count=10)
        
        result = classify_diff(diff)
        
        assert len(result.events) == 0
        assert result.total_changes == 0
    
    def test_multiple_changes(self, snapshot_a_id, snapshot_b_id):
        """Multiple changes should each produce an event."""
        added_id = uuid4()
        removed_id = uuid4()
        modified = make_modified_item(changes=[
            make_field_change("QUANTITY_CHANGED", from_value=5, to_value=10)
        ])
        
        diff = make_diff_result(
            snapshot_a_id, snapshot_b_id,
            added_items=[added_id],
            removed_items=[removed_id],
            modified_items=[modified]
        )
        
        result = classify_diff(diff)
        
        assert len(result.events) == 3
        assert result.added_count == 1
        assert result.removed_count == 1
        assert result.modified_count == 1
        
        # Check event types
        types = {e.event_type for e in result.events}
        assert ChangeEventType.PART_ADDED in types
        assert ChangeEventType.PART_REMOVED in types
        assert ChangeEventType.QUANTITY_CHANGED in types
    
    def test_classification_result_filtering(self, snapshot_a_id, snapshot_b_id):
        """ClassificationResult filtering methods should work correctly."""
        added_id = uuid4()
        modified = make_modified_item(changes=[
            make_field_change("QUANTITY_CHANGED", from_value=5, to_value=10)
        ])
        
        diff = make_diff_result(
            snapshot_a_id, snapshot_b_id,
            added_items=[added_id],
            modified_items=[modified]
        )
        
        result = classify_diff(diff)
        
        # Filter by type
        added_events = result.events_by_type(ChangeEventType.PART_ADDED)
        assert len(added_events) == 1
        
        # Filter by severity
        high_events = result.events_by_severity(Severity.HIGH)
        assert len(high_events) == 1
        assert high_events[0].event_type == ChangeEventType.PART_ADDED
        
        # Filter by domain
        procurement_events = result.events_by_domain(Domain.PROCUREMENT)
        assert len(procurement_events) == 2  # PART_ADDED and QUANTITY_CHANGED
    
    def test_classify_and_summarize(self, snapshot_a_id, snapshot_b_id):
        """classify_and_summarize should produce correct summary."""
        modified = make_modified_item(changes=[
            make_field_change("QUANTITY_CHANGED", from_value=5, to_value=10)
        ])
        diff = make_diff_result(
            snapshot_a_id, snapshot_b_id,
            modified_items=[modified]
        )
        
        summary = classify_and_summarize(diff)
        
        assert summary["total_events"] == 1
        assert "QUANTITY_CHANGED" in summary["events_by_type"]
        assert summary["events_by_type"]["QUANTITY_CHANGED"] == 1
        assert "MEDIUM" in summary["events_by_severity"]
    
    def test_get_high_priority_events(self, snapshot_a_id, snapshot_b_id):
        """get_high_priority_events should return only HIGH severity."""
        added_id = uuid4()
        modified = make_modified_item(changes=[
            make_field_change("QUANTITY_CHANGED", from_value=5, to_value=10)
        ])
        
        diff = make_diff_result(
            snapshot_a_id, snapshot_b_id,
            added_items=[added_id],  # HIGH severity
            modified_items=[modified]  # MEDIUM severity
        )
        
        high_events = get_high_priority_events(diff)
        
        assert len(high_events) == 1
        assert high_events[0].event_type == ChangeEventType.PART_ADDED
    
    def test_get_procurement_events(self, snapshot_a_id, snapshot_b_id):
        """get_procurement_events should filter by PROCUREMENT domain."""
        added_id = uuid4()
        modified_qty = make_modified_item(changes=[
            make_field_change("QUANTITY_CHANGED", from_value=5, to_value=10)
        ])
        modified_refdes = make_modified_item(changes=[
            make_field_change("ATTRIBUTE_CHANGED", field="reference_designator",
                            from_value="R1,R2", to_value="R1")
        ])
        
        diff = make_diff_result(
            snapshot_a_id, snapshot_b_id,
            added_items=[added_id],  # PROCUREMENT
            modified_items=[modified_qty, modified_refdes]  # PROCUREMENT, MANUFACTURING
        )
        
        procurement_events = get_procurement_events(diff)
        
        # PART_ADDED and QUANTITY_CHANGED affect procurement
        # REFERENCE_DESIGNATOR_CHANGED only affects manufacturing
        assert len(procurement_events) == 2


# =============================================================================
# DETERMINISM TESTS
# =============================================================================

class TestDeterminism:
    """Tests that classification is deterministic."""
    
    def test_same_input_same_output(self, snapshot_a_id, snapshot_b_id):
        """Same inputs should always produce same outputs."""
        bom_id = uuid4()
        
        for _ in range(5):
            modified = ModifiedItem(
                bom_item_id=bom_id,
                changes=[
                    FieldChange(
                        type="QUANTITY_CHANGED",
                        field=None,
                        from_value=5,
                        to_value=10
                    )
                ]
            )
            diff = DiffResult(
                snapshot_a_id=snapshot_a_id,
                snapshot_b_id=snapshot_b_id,
                added_items=[],
                removed_items=[],
                modified_items=[modified],
                unchanged_count=0
            )
            
            result = classify_diff(diff)
            
            assert len(result.events) == 1
            assert result.events[0].event_type == ChangeEventType.QUANTITY_CHANGED
            assert result.events[0].bom_item_id == bom_id


# =============================================================================
# SERIALIZATION TESTS
# =============================================================================

class TestSerialization:
    """Tests for serialization methods."""
    
    def test_change_event_to_dict(self, snapshot_a_id, snapshot_b_id):
        """ChangeEvent.to_dict() should produce valid dictionary."""
        modified = make_modified_item(changes=[
            make_field_change("QUANTITY_CHANGED", from_value=5, to_value=10)
        ])
        diff = make_diff_result(
            snapshot_a_id, snapshot_b_id,
            modified_items=[modified]
        )
        
        result = classify_diff(diff)
        event_dict = result.events[0].to_dict()
        
        assert "bom_item_id" in event_dict
        assert event_dict["event_type"] == "QUANTITY_CHANGED"
        assert event_dict["severity"] == "MEDIUM"
        assert "PROCUREMENT" in event_dict["affected_domains"]
        assert len(event_dict["evidence"]) == 1
    
    def test_classification_result_to_dict(self, snapshot_a_id, snapshot_b_id):
        """ClassificationResult.to_dict() should produce valid dictionary."""
        modified = make_modified_item(changes=[
            make_field_change("QUANTITY_CHANGED", from_value=5, to_value=10)
        ])
        diff = make_diff_result(
            snapshot_a_id, snapshot_b_id,
            modified_items=[modified]
        )
        
        result = classify_diff(diff)
        result_dict = result.to_dict()
        
        assert "snapshot_a_id" in result_dict
        assert "snapshot_b_id" in result_dict
        assert result_dict["total_changes"] == 1
        assert len(result_dict["events"]) == 1

