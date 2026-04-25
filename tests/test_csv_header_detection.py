"""Tests for CSV header detection and normalization robustness."""

import sys
from pathlib import Path

# Add parent directory to path to import bomkit
sys.path.insert(0, str(Path(__file__).parent.parent))

from bomkit import BomParser
from bomkit.adapters.csv_adapter import CsvAdapter


def test_header_detection_with_preamble_rows():
    """Ensure header detection skips preamble rows and maps correctly."""
    test_dir = Path(__file__).parent
    csv_file = test_dir / "Desktop FDM 3D Printer.csv"

    parser = BomParser(normalize=True)
    parser.register_adapter(CsvAdapter())

    rows = parser.parse(str(csv_file))
    assert rows, "No rows parsed from Desktop FDM 3D Printer.csv"

    first = rows[0]
    assert first["part_number"] == "FRM-ALU-2040"
    assert first["quantity"] == "4"
    assert first["unit"].lower() in {"ea", "each", "unit", "units"}
    assert first.get("supplier", "").lower() in {"misumi uk"}


def test_normalization_of_bom_4():
    """Ensure BOM-4.csv normalizes key columns."""
    test_dir = Path(__file__).parent
    csv_file = test_dir / "BOM-4.csv"

    parser = BomParser(normalize=True)
    parser.register_adapter(CsvAdapter())

    rows = parser.parse(str(csv_file))
    assert rows, "No rows parsed from BOM-4.csv"

    first = rows[0]
    assert first["reference_designator"].startswith("C1")
    assert first["quantity"] == "3"
    assert first["value"] == "10n"
    assert first["package"]
