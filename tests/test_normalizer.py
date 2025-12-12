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


def test_export_functionality():
    """Test export functionality."""
    
    test_dir = Path(__file__).parent
    csv_file = test_dir / "BOM-3.csv"
    
    parser = BomParser(normalize=True)
    parser.register_adapter(CsvAdapter())
    
    # Parse the file
    normalized_rows = parser.parse(str(csv_file))
    
    print(f"\n{'='*60}")
    print("Testing Export Functionality:")
    print(f"{'='*60}")
    
    # Test CSV export
    csv_output = test_dir / "BOM-3_normalized.csv"
    if csv_output.exists():
        csv_output.unlink()  # Remove if exists
    
    exported_path = parser.export(normalized_rows, str(csv_output))
    assert csv_output.exists(), "CSV export file was not created"
    print(f"✓ CSV export successful: {exported_path}")
    
    # Verify CSV export by reading it back
    parser_csv = BomParser(normalize=False)
    parser_csv.register_adapter(CsvAdapter())
    exported_data = parser_csv.parse(str(csv_output))
    assert len(exported_data) == len(normalized_rows), "Exported CSV row count mismatch"
    print(f"✓ CSV export verification: {len(exported_data)} rows match")
    
    # Test Excel export
    excel_output = test_dir / "BOM-3_normalized.xlsx"
    if excel_output.exists():
        excel_output.unlink()  # Remove if exists
    
    exported_path = parser.export(normalized_rows, str(excel_output), format='excel')
    assert excel_output.exists(), "Excel export file was not created"
    print(f"✓ Excel export successful: {exported_path}")
    
    # Test JSON export
    json_output = test_dir / "BOM-3_normalized.json"
    if json_output.exists():
        json_output.unlink()  # Remove if exists
    
    exported_path = parser.export(normalized_rows, str(json_output), format='json')
    assert json_output.exists(), "JSON export file was not created"
    print(f"✓ JSON export successful: {exported_path}")
    
    # Test parse_and_export convenience method
    combined_output = test_dir / "BOM-3_combined.csv"
    if combined_output.exists():
        combined_output.unlink()
    
    exported_path = parser.parse_and_export(str(csv_file), str(combined_output))
    assert combined_output.exists(), "Combined parse_and_export file was not created"
    print(f"✓ parse_and_export successful: {exported_path}")
    
    # Clean up test files
    for test_file in [csv_output, excel_output, json_output, combined_output]:
        if test_file.exists():
            test_file.unlink()
            print(f"  Cleaned up: {test_file.name}")
    
    print(f"\n✓ All export tests passed!")


def test_reference_designator_normalization():
    """Test reference designator range normalization."""
    
    from bomkit.normalizer import BomNormalizer
    
    normalizer = BomNormalizer()
    
    print(f"\n{'='*60}")
    print("Testing Reference Designator Normalization:")
    print(f"{'='*60}")
    
    test_cases = [
        ("R1, R2, R3", "R1-R3"),
        ("D1, D2, D3, D4, D5, D6, D7, D8", "D1-D8"),
        ("C1, C2, C4", "C1-C2, C4"),
        ("R1-R5", "R1-R5"),  # Already formatted
        ("R1, R3, R5", "R1, R3, R5"),  # Non-consecutive
        ("R1, R2, R3, R5, R6", "R1-R3, R5-R6"),  # Multiple ranges
        ("U1", "U1"),  # Single item
        ("R1, R2,", "R1-R2"),  # Trailing comma
        ("R10, R11, R12", "R10-R12"),  # Multi-digit numbers
    ]
    
    all_passed = True
    for input_val, expected in test_cases:
        result = normalizer.normalize_reference_designator(input_val)
        passed = result == expected
        status = "✓" if passed else "✗"
        if not passed:
            all_passed = False
        print(f"{status} '{input_val:30s}' -> '{result:30s}' (expected: '{expected}')")
    
    # Test with actual BOM data
    print(f"\n{'='*60}")
    print("Testing with BOM-3.csv data:")
    print(f"{'='*60}")
    
    test_dir = Path(__file__).parent
    csv_file = test_dir / "BOM-3.csv"
    
    parser = BomParser(normalize=True)
    parser.register_adapter(CsvAdapter())
    
    data = parser.parse(str(csv_file))
    
    # Check specific known cases
    expected_ranges = {
        "D1-D8": "D1, D2, D3, D4, D5, D6, D7, D8,",
        "R5-R12": "R5, R6, R7, R8, R9, R10, R11, R12,",
        "R1-R2": "R1, R2,",
    }
    
    for normalized_range, original in expected_ranges.items():
        # Find row with this original value
        found = False
        for row in data:
            # Check if this row's original would produce the normalized range
            # We need to check the actual normalized output
            if normalized_range in row["reference_designator"]:
                print(f"✓ Found normalized range '{normalized_range}' in output")
                found = True
                break
        if not found:
            print(f"✗ Expected range '{normalized_range}' not found")
            all_passed = False
    
    if all_passed:
        print(f"\n✓ All reference designator normalization tests passed!")
    else:
        print(f"\n✗ Some tests failed")
    
    return all_passed


if __name__ == "__main__":
    # Run the main test
    normalized_rows = test_bom_normalization()
    
    # Run comparison test
    test_raw_vs_normalized()
    
    # Run export test
    test_export_functionality()
    
    # Run reference designator normalization test
    test_reference_designator_normalization()
    
    print("\n" + "="*60)
    print("All tests completed successfully!")
    print("="*60 + "\n")

