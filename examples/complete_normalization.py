#!/usr/bin/env python3
"""Example: Complete BOM normalization using all bomkit features.

This script demonstrates how to use bomkit to transform a messy BOM file
into a perfectly normalized, standardized BOM using a single command.
"""

from bomkit import BomParser
from bomkit.adapters.csv_adapter import CsvAdapter
from bomkit.adapters.excel_adapter import ExcelAdapter


def normalize_bom(input_file: str, output_file: str):
    """Normalize a BOM file using all bomkit features.
    
    This single function call:
    1. Parses the input file (CSV, Excel, or PDF)
    2. Normalizes column names using lexical similarity
    3. Uses column profiling for ambiguous columns
    4. Normalizes units to SI engineering units
    5. Normalizes reference designators
    6. Exports to standardized format
    
    Args:
        input_file: Path to input BOM file
        output_file: Path to output normalized BOM file
    """
    # Initialize parser with all features enabled
    parser = BomParser(
        normalize=True,                    # Enable column normalization
        normalize_units=True,             # Enable unit normalization
        use_lexical_similarity=True,      # Enable lexical matching (handles typos, abbreviations)
        similarity_threshold=0.6,         # Minimum similarity score for lexical matching
        use_column_profiling=True,        # Enable column profiling (disambiguates ambiguous columns)
        profile_similarity_threshold=0.7  # Minimum profile similarity score
    )
    
    # Register file adapters
    parser.register_adapter(CsvAdapter())
    parser.register_adapter(ExcelAdapter())
    
    # Single command to normalize everything!
    normalized_bom = parser.normalize_complete(
        input_path=input_file,
        output_path=output_file
    )
    
    print(f"✓ Successfully normalized {len(normalized_bom)} rows")
    print(f"✓ Output saved to: {output_file}")
    
    # Show mapping report
    report = parser.get_mapping_report(input_file)
    print(f"\nColumn Mapping Report:")
    print(f"  Mapped columns: {len(report['mapped'])}")
    print(f"  Unmapped columns: {len(report['unmapped'])}")
    
    if report['unmapped']:
        print(f"  Unmapped: {', '.join(report['unmapped'])}")
    
    return normalized_bom


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 3:
        print("Usage: python complete_normalization.py <input_file> <output_file>")
        print("\nExample:")
        print("  python complete_normalization.py messy_bom.csv clean_bom.xlsx")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    normalize_bom(input_file, output_file)

