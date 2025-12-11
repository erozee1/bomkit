"""Test suite for BOM normalization functionality."""

import os
import sys
from pathlib import Path

# Add parent directory to path to import bomkit
sys.path.insert(0, str(Path(__file__).parent.parent))

from bomkit import BomParser, BomNormalizer, STANDARD_HEADERS
from bomkit.adapters.csv_adapter import CsvAdapter


def test_bom_normalization():
    """Test normalization of BOM-3.csv file."""
    
    # Get the path to the test CSV file
    test_dir = Path(__file__).parent
    csv_file = test_dir / "BOM-3.csv"
    
    # Verify file exists
    assert csv_file.exists(), f"Test file not found: {csv_file}"
    
    print(f"\n{'='*60}")
    print(f"Testing BOM Normalization")
    print(f"{'='*60}")
    print(f"Test file: {csv_file}")
    print(f"\nStandard BOM Template Headers:")
    for i, header in enumerate(STANDARD_HEADERS, 1):
        print(f"  {i:2d}. {header}")
    
    # Initialize parser with normalization
    parser = BomParser(normalize=True)
    parser.register_adapter(CsvAdapter())
    
    # Parse and normalize the file
    print(f"\n{'='*60}")
    print("Parsing and normalizing BOM file...")
    print(f"{'='*60}")
    
    normalized_rows = parser.parse(str(csv_file))
    
    print(f"\n✓ Successfully parsed {len(normalized_rows)} rows")
    
    # Display first few normalized rows
    print(f"\n{'='*60}")
    print("Sample Normalized Rows (first 3):")
    print(f"{'='*60}")
    
    for i, row in enumerate(normalized_rows[:3], 1):
        print(f"\nRow {i}:")
        for header in STANDARD_HEADERS:
            value = row.get(header, "")
            if value:  # Only show non-empty values
                print(f"  {header:25s}: {value}")
    
    # Verify all rows have standard headers
    print(f"\n{'='*60}")
    print("Verification:")
    print(f"{'='*60}")
    
    all_have_standard_headers = True
    for i, row in enumerate(normalized_rows):
        for header in STANDARD_HEADERS:
            if header not in row:
                print(f"✗ Row {i+1} missing header: {header}")
                all_have_standard_headers = False
    
    if all_have_standard_headers:
        print("✓ All rows have standard headers")
    
    # Get mapping report
    print(f"\n{'='*60}")
    print("Column Mapping Report:")
    print(f"{'='*60}")
    
    report = parser.get_mapping_report(str(csv_file))
    
    print("\nMapped columns:")
    for standard_header, variations in report["mapped"].items():
        print(f"  {standard_header:25s} <- {', '.join(variations)}")
    
    if report["unmapped"]:
        print(f"\nUnmapped columns (added to 'notes'):")
        for col in report["unmapped"]:
            print(f"  - {col}")
    else:
        print("\n✓ All columns were successfully mapped")
    
    # Summary statistics
    print(f"\n{'='*60}")
    print("Summary Statistics:")
    print(f"{'='*60}")
    
    total_parts = len(normalized_rows)
    parts_with_manufacturer = sum(1 for row in normalized_rows if row.get("manufacturer"))
    parts_with_mpn = sum(1 for row in normalized_rows if row.get("manufacturer_part_number"))
    parts_with_ref_des = sum(1 for row in normalized_rows if row.get("reference_designator"))
    
    print(f"Total parts: {total_parts}")
    print(f"Parts with manufacturer: {parts_with_manufacturer}")
    print(f"Parts with MPN: {parts_with_mpn}")
    print(f"Parts with reference designator: {parts_with_ref_des}")
    
    # Verify normalization worked
    assert len(normalized_rows) > 0, "No rows were parsed"
    assert all_have_standard_headers, "Not all rows have standard headers"
    
    print(f"\n{'='*60}")
    print("✓ All tests passed!")
    print(f"{'='*60}\n")
    
    return normalized_rows


def test_raw_vs_normalized():
    """Compare raw and normalized output."""
    
    test_dir = Path(__file__).parent
    csv_file = test_dir / "BOM-3.csv"
    
    parser = BomParser(normalize=False)
    parser.register_adapter(CsvAdapter())
    
    raw_rows = parser.parse(str(csv_file))
    
    parser_normalized = BomParser(normalize=True)
    parser_normalized.register_adapter(CsvAdapter())
    
    normalized_rows = parser_normalized.parse(str(csv_file))
    
    print(f"\n{'='*60}")
    print("Raw vs Normalized Comparison:")
    print(f"{'='*60}")
    
    if raw_rows:
        print(f"\nRaw columns (first row):")
        for key in raw_rows[0].keys():
            print(f"  - {key}")
    
    print(f"\nNormalized columns (standard template):")
    for header in STANDARD_HEADERS:
        print(f"  - {header}")
    
    assert len(raw_rows) == len(normalized_rows), "Row count mismatch"
    print(f"\n✓ Row counts match: {len(raw_rows)} rows")


if __name__ == "__main__":
    # Run the main test
    normalized_rows = test_bom_normalization()
    
    # Run comparison test
    test_raw_vs_normalized()
    
    print("\n" + "="*60)
    print("All tests completed successfully!")
    print("="*60 + "\n")

