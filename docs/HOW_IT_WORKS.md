# How bomkit Works: Complete Normalization Pipeline

## Overview

bomkit transforms messy, inconsistent Bill of Materials (BOM) files into perfectly standardized documents. This document explains how all the features work together.

## The Single Command

```python
parser.normalize_complete(input_path, output_path)
```

This one command orchestrates the entire normalization pipeline.

## Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    INPUT: Messy BOM File                     │
│              (CSV, Excel, or PDF with typos,                 │
│               inconsistent columns, mixed units)             │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│  STAGE 1: File Parsing                                       │
│  ────────────────────────                                    │
│  • CSVAdapter: Handles CSV/TSV with encoding detection      │
│  • ExcelAdapter: Reads .xlsx/.xls files                       │
│  • PDFAdapter: Extracts tables from PDF                      │
│                                                               │
│  Output: List of dictionaries (raw rows)                     │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│  STAGE 2: Column Name Normalization                          │
│  ────────────────────────────────────────                     │
│                                                               │
│  Step 2.1: Exact Matching                                    │
│  ────────────────────────                                    │
│  • Direct lookup in COLUMN_MAPPINGS                          │
│  • "qty" → "quantity"                                        │
│  • "part number" → "part_number"                             │
│                                                               │
│  Step 2.2: Lexical Similarity (for natural language cols)   │
│  ────────────────────────────────────────────                │
│  Applies to: description, quantity, unit, manufacturer, notes│
│  NOT applied to: part_number, manufacturer_part_number, etc. │
│                                                               │
│  Features:                                                    │
│  • Normalization: lowercase, strip punctuation, split camelCase│
│  • Jaro-Winkler: Handles typos ("mpn" vs "mnp")              │
│  • Token Similarity: Jaccard & Cosine                        │
│  • Abbreviation Expansion: qty→quantity, mfg→manufacturer    │
│  • Spell-Check: quantitiy→quantity, manufaturer→manufacturer  │
│                                                               │
│  Step 2.3: Column Profiling (for ambiguous columns)        │
│  ──────────────────────────────────────────────              │
│  When lexical matching fails, analyzes data patterns:        │
│  • Type distribution (numeric vs text)                       │
│  • Pattern matching (MPN patterns, ref des patterns)       │
│  • Unit detection (V, A, uF, Ω)                             │
│  • Length statistics                                         │
│                                                               │
│  Example: "Value" column with units → electrical value      │
│                                                               │
│  Output: Standardized column names                           │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│  STAGE 3: Data Normalization                                 │
│  ─────────────────────────────                               │
│                                                               │
│  3.1 Unit Normalization                                      │
│  ────────────────────                                        │
│  • Converts to SI engineering units                          │
│  • "10nF" → 1e-8 (Farads)                                   │
│  • "1kΩ" → 1000 (Ohms)                                       │
│  • "3.3V" → 3.3 (Volts)                                     │
│                                                               │
│  3.2 Reference Designator Normalization                      │
│  ────────────────────────────────────                        │
│  • "R1, R2, R3" → "R1-R3"                                   │
│  • "C1, C2, C4" → "C1, C2, C4" (non-consecutive)            │
│                                                               │
│  3.3 Data Cleaning                                           │
│  ────────────────                                            │
│  • Strip whitespace                                           │
│  • Handle empty values                                        │
│  • Preserve unmapped columns in notes                        │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│  STAGE 4: Export                                             │
│  ─────────────                                               │
│  • Standard template headers in consistent order            │
│  • Formats: CSV, Excel, JSON                                │
│                                                               │
│  Output: Perfectly normalized BOM                             │
└─────────────────────────────────────────────────────────────┘
```

## Feature Interaction

### Why Three Matching Techniques?

1. **Exact Matching** (fastest)
   - For known variations in COLUMN_MAPPINGS
   - No ambiguity, instant match

2. **Lexical Similarity** (for natural language)
   - Handles typos, abbreviations, case variations
   - Only for columns where fuzzy matching is safe
   - Excludes part numbers (too complex/non-natural)

3. **Column Profiling** (for ambiguous cases)
   - When column name is ambiguous (e.g., "Value")
   - Analyzes actual data to determine meaning
   - Uses statistical patterns, not just names

### Example: Handling "quantitiy" Column

1. **Exact match fails**: "quantitiy" not in COLUMN_MAPPINGS
2. **Lexical similarity kicks in**:
   - Normalizes: "quantitiy" → "quantitiy" (lowercase)
   - Spell-check: "quantitiy" → "quantity" (corrected)
   - Matches against "quantity" variations
   - Jaro-Winkler similarity: 0.95
   - **Result**: Maps to "quantity" ✅

### Example: Handling "Value" Column

1. **Exact match fails**: "Value" could mean multiple things
2. **Lexical similarity**: Matches "value" standard column
3. **Column profiling verifies**:
   - Checks if values contain units (V, A, uF, Ω)
   - If yes → electrical value ✅
   - If no → might be cost or other (needs more context)

### Example: Handling "Part" Column

1. **Exact match fails**: "Part" is ambiguous
2. **Lexical similarity**: Could match "part_number" or "description"
3. **Column profiling disambiguates**:
   - Checks for MPN patterns (alphanumeric, specific format)
   - Checks length (descriptions are longer)
   - Checks type (text vs. alphanumeric)
   - **Result**: Maps to appropriate column ✅

## Key Design Decisions

### Why Lexical Similarity Only for Some Columns?

**Applied to:**
- `description` - Natural language
- `quantity` - Natural language label
- `unit` - Natural language label
- `manufacturer` - Natural language
- `notes` - Natural language

**NOT Applied to:**
- `part_number` - Complex patterns, non-natural language
- `manufacturer_part_number` - Complex patterns, non-natural language
- `reference_designator` - Structured format (R1, C2, etc.)
- `value` - Can be numeric or unit-based (profiling handles this)
- `package` - Technical identifiers

**Reason**: Fuzzy matching on part numbers would cause false positives. "STM32F103" and "STM32F104" are different parts, not typos!

### Why Column Profiling?

Some column names are inherently ambiguous:
- "Value" → electrical value? cost? declared value?
- "Part" → part number? description? manufacturer part number?
- "Number" → part number? quantity? reference designator?

Profiling analyzes the **data** to determine meaning, not just the name.

## Performance

- **Exact matching**: O(1) - instant
- **Lexical similarity**: O(n) where n = number of known variations (small, ~10-20 per column)
- **Column profiling**: O(m) where m = number of rows (analyzes all data)
- **Caching**: Profiles are cached to avoid re-computation

## Configuration

All features are configurable:

```python
parser = BomParser(
    normalize=True,                    # Enable/disable normalization
    normalize_units=True,             # Enable/disable unit normalization
    use_lexical_similarity=True,      # Enable/disable lexical matching
    similarity_threshold=0.6,        # Minimum similarity score (0-1)
    use_column_profiling=True,       # Enable/disable profiling
    profile_similarity_threshold=0.7 # Minimum profile similarity
)
```

**Threshold Guidelines:**
- Lower threshold (0.5-0.6): More matches, risk of false positives
- Higher threshold (0.7-0.8): Fewer matches, more conservative
- Default (0.6): Balanced for most use cases

## Summary

bomkit uses a **layered approach** to normalization:

1. **Fast exact matching** for known variations
2. **Intelligent fuzzy matching** for natural language columns with typos
3. **Statistical analysis** for ambiguous columns
4. **Unit and format normalization** for data consistency

This ensures that even the messiest BOM files can be transformed into perfectly standardized documents ready for downstream processing, analysis, or integration.

