#!/usr/bin/env python3
"""
Quick test of diff functionality using the two snapshots from test_ingest.
"""

import os
import sys
from pathlib import Path
from uuid import UUID

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Load .env file
try:
    from dotenv import load_dotenv
    load_dotenv(project_root / ".env")
except ImportError:
    env_path = project_root / ".env"
    if env_path.exists():
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip()

from bomkit.ingest import SupabaseClient, diff_snapshots, DiffResult


def print_diff_result(diff: DiffResult, db: SupabaseClient):
    """Pretty print a diff result with human-readable details."""
    print("=" * 60)
    print("Snapshot Diff Result")
    print("=" * 60)
    
    # Get snapshot info
    snapshot_a_info = db.get_snapshot_info(diff.snapshot_a_id)
    snapshot_b_info = db.get_snapshot_info(diff.snapshot_b_id)
    
    print(f"Snapshot A: {diff.snapshot_a_id}")
    print(f"  Assembly: {snapshot_a_info.get('assembly_name', 'Unknown')}")
    print(f"  Created: {snapshot_a_info.get('created_at', 'Unknown')}")
    print()
    print(f"Snapshot B: {diff.snapshot_b_id}")
    print(f"  Assembly: {snapshot_b_info.get('assembly_name', 'Unknown')}")
    print(f"  Created: {snapshot_b_info.get('created_at', 'Unknown')}")
    print()
    
    print(f"📊 Summary:")
    print(f"  Added items: {len(diff.added_items)}")
    print(f"  Removed items: {len(diff.removed_items)}")
    print(f"  Modified items: {len(diff.modified_items)}")
    print(f"  Unchanged items: {diff.unchanged_count}")
    print()
    
    # Get all bom_item details we need
    all_bom_item_ids = diff.added_items + diff.removed_items + [m.bom_item_id for m in diff.modified_items]
    bom_item_details = db.get_bom_item_details(all_bom_item_ids) if all_bom_item_ids else {}
    
    if diff.added_items:
        print(f"➕ Added Items ({len(diff.added_items)}):")
        for bom_item_id in diff.added_items:
            details = bom_item_details.get(bom_item_id, {})
            part_name = details.get('part_name', 'Unknown')
            context = details.get('context', {})
            refdes = context.get('reference_designator', 'N/A')
            print(f"  • {part_name} (RefDes: {refdes})")
            if details.get('part_attributes'):
                attrs = details['part_attributes']
                if attrs.get('value'):
                    print(f"    Value: {attrs['value']}")
                if attrs.get('package'):
                    print(f"    Package: {attrs['package']}")
            print(f"    BOM Item ID: {bom_item_id}")
        print()
    
    if diff.removed_items:
        print(f"➖ Removed Items ({len(diff.removed_items)}):")
        for bom_item_id in diff.removed_items:
            details = bom_item_details.get(bom_item_id, {})
            part_name = details.get('part_name', 'Unknown')
            context = details.get('context', {})
            refdes = context.get('reference_designator', 'N/A')
            print(f"  • {part_name} (RefDes: {refdes})")
            if details.get('part_attributes'):
                attrs = details['part_attributes']
                if attrs.get('value'):
                    print(f"    Value: {attrs['value']}")
                if attrs.get('package'):
                    print(f"    Package: {attrs['package']}")
            print(f"    BOM Item ID: {bom_item_id}")
        print()
    
    if diff.modified_items:
        print(f"🔄 Modified Items ({len(diff.modified_items)}):")
        for modified in diff.modified_items:
            details = bom_item_details.get(modified.bom_item_id, {})
            part_name = details.get('part_name', 'Unknown')
            
            # Get reference_designator from snapshot_items.attributes (not bom_items.context)
            # Check if refdes is in the changes
            refdes_a = None
            refdes_b = None
            for change in modified.changes:
                if change.field == "reference_designator":
                    refdes_a = change.from_value
                    refdes_b = change.to_value
                    break
            
            # If refdes changed, show both; otherwise show current refdes from details
            if refdes_a and refdes_b:
                print(f"  • {part_name} (RefDes: {refdes_a} → {refdes_b})")
            else:
                # Try to get refdes from context (legacy) or from snapshot state
                context = details.get('context', {})
                refdes = context.get('reference_designator', 'N/A')
                print(f"  • {part_name} (RefDes: {refdes})")
            
            for change in modified.changes:
                if change.type == "QUANTITY_CHANGED":
                    print(f"    📦 Quantity: {change.from_value} → {change.to_value}")
                elif change.type == "ATTRIBUTE_CHANGED":
                    # Special handling for reference_designator (important for engineers)
                    if change.field == "reference_designator":
                        print(f"    🔌 Reference Designator: {change.from_value} → {change.to_value}")
                    else:
                        print(f"    🔧 {change.field}: {change.from_value} → {change.to_value}")
                elif change.type == "ATTRIBUTE_ADDED":
                    print(f"    ➕ {change.field}: (added) → {change.to_value}")
                elif change.type == "ATTRIBUTE_REMOVED":
                    print(f"    ➖ {change.field}: {change.from_value} → (removed)")
            print(f"    BOM Item ID: {modified.bom_item_id}")
            print()
    
    if diff.unchanged_count > 0:
        print(f"✓ Unchanged: {diff.unchanged_count} items")


def main():
    """Test diff with the two snapshots from test_ingest."""
    
    # Snapshot IDs from your test runs
    snapshot_a_id = UUID("d72174e2-30e8-4c68-a7ae-4b62c31ffd11")  # BOM-3 (13 rows)
    snapshot_b_id = UUID("41b173ff-6a92-452c-86b1-21cf485345e7")  # BOM-4 (12 rows)
    
    print("=" * 60)
    print("Testing Snapshot Diff")
    print("=" * 60)
    print(f"Snapshot A (BOM-3): {snapshot_a_id}")
    print(f"Snapshot B (BOM-4): {snapshot_b_id}")
    print()
    
    # Get connection
    db_url = os.getenv("SUPABASE_DB_URL")
    if not db_url:
        print("❌ Error: SUPABASE_DB_URL not set")
        return
    
    db = SupabaseClient(db_url=db_url)
    
    try:
        print("🔍 Computing diff...")
        print()
        
        # Perform diff
        diff = diff_snapshots(
            snapshot_a_id=snapshot_a_id,
            snapshot_b_id=snapshot_b_id,
            db=db
        )
        
        # Print results with detailed information
        print_diff_result(diff, db)
        
        print()
        print("✅ Diff completed successfully!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        db.close()


if __name__ == "__main__":
    main()

