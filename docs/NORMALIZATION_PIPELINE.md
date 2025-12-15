# BOM Normalization Pipeline

This document explains how bomkit's different features work together to transform messy, inconsistent Bill of Materials (BOM) files into perfectly normalized, standardized documents.

## Overview

The bomkit normalization pipeline consists of four main stages:

1. **File Parsing** - Reads BOM files from various formats (CSV, Excel, PDF)
2. **Column Name Normalization** - Maps inconsistent column names to standard headers
3. **Data Normalization** - Normalizes units, reference designators, and data formats
4. **Export** - Outputs standardized BOM in desired format

## Stage 1: File Parsing

**Components:** File Adapters (CSV, Excel, PDF)

The parser uses adapter pattern to handle different file formats:

- **CSVAdapter**: Handles CSV/TSV files with automatic encoding detection (UTF-8, Windows-1252, etc.) and delimiter detection (comma, semicolon, tab)
- **ExcelAdapter**: Reads Excel files (.xlsx, .xls)
- **PDFAdapter**: Extracts tables from PDF files

**What it does:**
- Detects file format automatically
- Handles encoding issues (common with Excel exports)
- Extracts raw data as list of dictionaries

## Stage 2: Column Name Normalization

This is the most complex stage, using three complementary techniques:

### 2.1 Exact Matching
**Component:** `BomNormalizer` with `COLUMN_MAPPINGS`

First, tries exact matches against known column name variations:
- "part number" → "part_number"
- "qty" → "quantity"
- "mfg" → "manufacturer"

### 2.2 Lexical Similarity Matching
**Component:** `LexicalSimilarity`

For headers and natural language columns (description, quantity, unit, manufacturer, notes), uses multiple lexical features:

#### Features Used:

1. **Text Normalization**
   - Lowercase conversion
   - Punctuation stripping
   - Snake_case and camelCase splitting
   - Token unification (mfr→manufacturer, qty→quantity)

2. **Edit Distance / Jaro-Winkler**
   - Handles typos in short labels (e.g., "mpn" vs "mnp")
   - Jaro-Winkler gives higher weight to common prefixes

3. **Token Similarity**
   - **Jaccard similarity**: Measures overlap of token sets
   - **Cosine similarity**: Measures vector similarity of token frequencies

4. **Abbreviation Expansion**
   - Curated dictionary expansion (qty→quantity, mfg→manufacturer)
   - Handles common engineering abbreviations

5. **Spell-Check**
   - Fast correction using edit distance
   - Handles common typos (quantitiy→quantity, manufaturer→manufacturer)

**Important:** Lexical similarity is **NOT** applied to:
- `part_number` (non-natural language, complex patterns)
- `manufacturer_part_number` (non-natural language)
- `reference_designator` (structured format)
- `value` (can be numeric or unit-based)
- `package` (technical identifiers)

These columns use strict matching only to avoid false positives.

### 2.3 Column Profiling
**Component:** `ColumnProfiler`

For ambiguous columns that can't be matched lexically, uses statistical/pattern analysis:

- **Type distribution**: Checks if column is mostly numeric, text, or mixed
- **Pattern matching**: Detects MPN patterns, reference designator patterns, unit patterns
- **Length statistics**: Analyzes value lengths to distinguish descriptions from part numbers
- **Unit presence**: Detects electrical units (V, A, uF, nF, Ω) to identify value columns

**Example:** A column named "Value" could be:
- Electrical value (has units like "10nF", "1kΩ") → maps to `value`
- Cost (numeric, no units) → might map to `value` or stay unmapped
- Declared value → maps to `value`

Profiling helps disambiguate these cases.

## Stage 3: Data Normalization

### 3.1 Unit Normalization
**Component:** `UnitNormalizer`

Converts all unit values to SI engineering units:
- "10nF" → 1e-8 (Farads)
- "1kΩ" → 1000 (Ohms)
- "3.3V" → 3.3 (Volts)

Uses the `pint` library for robust unit conversion.

### 3.2 Reference Designator Normalization
**Component:** `BomNormalizer.normalize_reference_designator()`

Standardizes reference designator formats:
- "R1, R2, R3" → "R1-R3" (consecutive ranges)
- "C1, C2, C4" → "C1, C2, C4" (non-consecutive)
- "D1-D8" → "D1-D8" (already formatted)

### 3.3 Data Cleaning
- Strips whitespace
- Handles empty values
- Preserves unmapped columns in `notes` field

## Stage 4: Export

**Component:** `BomParser.export()`

Outputs normalized BOM in desired format:
- CSV
- Excel (.xlsx)
- JSON

Uses standard template headers in consistent order.

## Complete Pipeline Example

```python
from bomkit import BomParser
from bomkit.adapters.csv_adapter import CsvAdapter
from bomkit.adapters.excel_adapter import ExcelAdapter

# Initialize parser with all features enabled
parser = BomParser(
    normalize=True,                    # Enable column normalization
    normalize_units=True,             # Enable unit normalization
    use_lexical_similarity=True,      # Enable lexical matching
    similarity_threshold=0.6,         # Minimum similarity score
    use_column_profiling=True,        # Enable column profiling
    profile_similarity_threshold=0.7  # Minimum profile similarity
)

# Register file adapters
parser.register_adapter(CsvAdapter())
parser.register_adapter(ExcelAdapter())

# Single command to normalize everything
normalized_bom = parser.normalize_complete(
    input_path='messy_bom.csv',
    output_path='clean_bom.xlsx'
)
```

## What Gets Fixed

### Column Name Issues:
- ✅ Typos: "quantitiy" → "quantity"
- ✅ Abbreviations: "qty" → "quantity", "mfg" → "manufacturer"
- ✅ Variations: "part number" → "part_number", "Part_Number" → "part_number"
- ✅ Case variations: "MANUFACTURER" → "manufacturer"
- ✅ Separator variations: "part-number" → "part_number"

### Data Issues:
- ✅ Unit inconsistencies: "10nF", "10 nF", "10n" → normalized values
- ✅ Reference designator formats: "R1, R2, R3" → "R1-R3"
- ✅ Missing columns: Added with empty values
- ✅ Extra columns: Moved to `notes` field

### Ambiguity Resolution:
- ✅ "Value" column with units → electrical value
- ✅ "Part" column with MPN pattern → manufacturer_part_number
- ✅ "Part" column with long text → description

## Standard Output Template

All normalized BOMs use this standard column order:

1. `part_number` - Internal part identifier
2. `description` - Component description
3. `quantity` - Number of units required
4. `unit` - Unit of measure (pcs, pieces, ea, each)
5. `manufacturer` - Manufacturer name
6. `manufacturer_part_number` - Manufacturer's part number
7. `reference_designator` - Schematic reference (e.g., "R1-R3")
8. `value` - Electrical value or specification
9. `package` - Package type/footprint
10. `notes` - Additional information

## Performance Considerations

- **Lexical similarity**: Fast for headers (small vocabulary)
- **Column profiling**: Analyzes all data, can be slower for large files
- **Caching**: Column profiles are cached for reuse
- **Thresholds**: Adjustable similarity thresholds balance accuracy vs. false positives

## Best Practices

1. **Use `normalize_complete()`** for the simplest workflow
2. **Adjust thresholds** if you get too many false positives (raise threshold) or miss matches (lower threshold)
3. **Check mapping reports** using `get_mapping_report()` to verify column mappings
4. **Review unmapped columns** - they may need to be added to `COLUMN_MAPPINGS`

## Architecture Summary

```
BomParser
├── File Adapters (CSV, Excel, PDF)
│   └── Raw data extraction
├── BomNormalizer
│   ├── Exact matching (COLUMN_MAPPINGS)
│   ├── LexicalSimilarity
│   │   ├── Normalization
│   │   ├── Jaro-Winkler / Edit Distance
│   │   ├── Token similarity (Jaccard, Cosine)
│   │   ├── Abbreviation expansion
│   │   └── Spell-check
│   └── ColumnProfiler (for ambiguous columns)
│       ├── Type distribution
│       ├── Pattern matching
│       ├── Length statistics
│       └── Unit detection
├── UnitNormalizer
│   └── SI unit conversion
└── Export
    └── Standardized output
```

This architecture ensures that even the messiest BOM files can be transformed into perfectly normalized, standardized documents ready for downstream processing.

