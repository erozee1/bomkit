"""Test suite for column profiling functionality."""

import os
import sys
from pathlib import Path

# Add parent directory to path to import bomkit
sys.path.insert(0, str(Path(__file__).parent.parent))

from bomkit import ColumnProfiler


def test_column_profiling():
    """Test column profiling with sample data."""
    
    print(f"\n{'='*60}")
    print("Testing Column Profiling")
    print(f"{'='*60}")
    
    profiler = ColumnProfiler(sample_size=200)
    
    # Test data simulating different column types
    test_data = {
        # MPN column - alphanumeric codes
        "MPN": [
            "RC0603FR-0710KL",
            "STM32F103C8T6",
            "LM358",
            "1N4148",
            "2N3904",
            "MAX232",
            "74LS138",
            "ATmega328P",
        ],
        
        # Reference designator column
        "RefDes": [
            "R1",
            "R2",
            "R3",
            "C1",
            "C2",
            "U1",
            "D1",
            "D2",
        ],
        
        # Value column with units (electrical values)
        "Value": [
            "10nF",
            "100nF",
            "1kΩ",
            "470R",
            "3.3V",
            "5V",
            "100kΩ",
            "22pF",
        ],
        
        # Description column - text
        "Description": [
            "Resistor, 10k ohm",
            "Capacitor, ceramic",
            "LED, red",
            "Microcontroller",
            "Voltage regulator",
            "Crystal oscillator",
            "Connector, header",
            "Diode, switching",
        ],
        
        # Quantity column - numeric
        "Quantity": [
            "1",
            "2",
            "3",
            "4",
            "5",
            "1",
            "2",
            "1",
        ],
        
        # Mixed type column
        "Mixed": [
            "10",
            "20",
            "ABC",
            "30",
            "XYZ",
            "40",
        ],
    }
    
    # Profile each column
    profiles = {}
    for column_name, values in test_data.items():
        profile = profiler.profile_column(column_name, values)
        profiles[column_name] = profile
        
        print(f"\n{'='*60}")
        print(f"Profile for '{column_name}':")
        print(f"{'='*60}")
        print(f"Sample size: {profile['sample_size']}")
        print(f"Null count: {profile['null_count']}")
        
        # Type distribution
        type_dist = profile.get('type_distribution', {})
        print(f"\nType Distribution:")
        print(f"  Numeric: {type_dist.get('numeric', 0):.2%}")
        print(f"  Text: {type_dist.get('text', 0):.2%}")
        print(f"  Mixed: {type_dist.get('mixed', 0):.2%}")
        
        # Regex hits
        regex_hits = profile.get('regex_hits', {})
        print(f"\nRegex Patterns:")
        print(f"  MPN-like: {regex_hits.get('mpn_like', 0):.2%}")
        print(f"  RefDes-like: {regex_hits.get('ref_des_like', 0):.2%}")
        
        # Unit presence
        unit_presence = profile.get('unit_presence', {})
        if unit_presence:
            print(f"\nUnit Presence:")
            for unit, ratio in sorted(unit_presence.items(), key=lambda x: x[1], reverse=True):
                if ratio > 0:
                    print(f"  {unit}: {ratio:.2%}")
        
        # Cardinality
        cardinality = profile.get('cardinality', {})
        print(f"\nCardinality:")
        print(f"  Unique ratio: {cardinality.get('unique_ratio', 0):.2%}")
        print(f"  Repeated ratio: {cardinality.get('repeated_ratio', 0):.2%}")
        print(f"  Unique count: {cardinality.get('unique_count', 0)}")
        
        # Length stats
        length_stats = profile.get('length_stats', {})
        print(f"\nLength Statistics:")
        print(f"  Mean: {length_stats.get('mean', 0):.2f}")
        print(f"  Median: {length_stats.get('median', 0):.2f}")
        print(f"  Min: {length_stats.get('min', 0)}")
        print(f"  Max: {length_stats.get('max', 0)}")
        
        # Character class stats
        char_stats = profile.get('character_class_stats', {})
        print(f"\nCharacter Class Statistics:")
        print(f"  Digits: {char_stats.get('percent_digits', 0):.2f}%")
        print(f"  Letters: {char_stats.get('percent_letters', 0):.2f}%")
        print(f"  Punctuation: {char_stats.get('percent_punctuation', 0):.2f}%")
        print(f"  Whitespace: {char_stats.get('percent_whitespace', 0):.2f}%")
    
    # Test profile comparison
    print(f"\n{'='*60}")
    print("Profile Comparison:")
    print(f"{'='*60}")
    
    # Compare MPN and Description (should be different)
    similarity1 = profiler.compare_profiles(profiles["MPN"], profiles["Description"])
    print(f"MPN vs Description similarity: {similarity1:.3f}")
    
    # Compare Value and Description (should be different)
    similarity2 = profiler.compare_profiles(profiles["Value"], profiles["Description"])
    print(f"Value vs Description similarity: {similarity2:.3f}")
    
    # Compare similar columns (should be more similar)
    similarity3 = profiler.compare_profiles(profiles["MPN"], profiles["RefDes"])
    print(f"MPN vs RefDes similarity: {similarity3:.3f}")
    
    # Test with actual BOM data
    print(f"\n{'='*60}")
    print("Testing with BOM-3.csv data:")
    print(f"{'='*60}")
    
    from bomkit.adapters.csv_adapter import CsvAdapter
    from bomkit import BomParser
    
    test_dir = Path(__file__).parent
    csv_file = test_dir / "BOM-3.csv"
    
    if csv_file.exists():
        parser = BomParser(normalize=False)
        parser.register_adapter(CsvAdapter())
        raw_rows = parser.parse(str(csv_file))
        
        # Profile all columns
        all_profiles = profiler.profile_dataframe(raw_rows)
        
        print(f"\nProfiled {len(all_profiles)} columns:")
        for column_name, profile in all_profiles.items():
            if 'error' not in profile:
                type_dist = profile.get('type_distribution', {})
                regex_hits = profile.get('regex_hits', {})
                unit_presence = profile.get('unit_presence', {})
                
                print(f"\n  {column_name}:")
                print(f"    Type: {type_dist}")
                print(f"    MPN-like: {regex_hits.get('mpn_like', 0):.2%}")
                print(f"    RefDes-like: {regex_hits.get('ref_des_like', 0):.2%}")
                if unit_presence:
                    units = [u for u, r in unit_presence.items() if r > 0.1]
                    if units:
                        print(f"    Units: {', '.join(units)}")
    else:
        print(f"  Test file not found: {csv_file}")
    
    print(f"\n{'='*60}")
    print("✓ Column profiling tests completed!")
    print(f"{'='*60}\n")


def test_ambiguous_column_matching():
    """Test matching of ambiguous columns using profiling."""
    
    print(f"\n{'='*60}")
    print("Testing Ambiguous Column Matching")
    print(f"{'='*60}")
    
    from bomkit import BomNormalizer
    
    # Create test data with ambiguous column names
    test_data = [
        {
            "Value": "10nF",  # Electrical value with unit
            "Part": "RC0603FR-0710KL",  # Could be MPN or part number
            "Description": "Resistor, 10k ohm",
        },
        {
            "Value": "100kΩ",
            "Part": "STM32F103C8T6",
            "Description": "Microcontroller",
        },
        {
            "Value": "3.3V",
            "Part": "LM358",
            "Description": "Op-amp",
        },
    ]
    
    normalizer = BomNormalizer(use_column_profiling=True)
    
    # Get profiles
    profiles = normalizer.get_column_profiles(test_data)
    
    print("\nColumn Profiles:")
    for column_name, profile in profiles.items():
        if 'error' not in profile:
            print(f"\n  {column_name}:")
            print(f"    Type: {profile.get('type_distribution', {})}")
            print(f"    Units: {list(profile.get('unit_presence', {}).keys())}")
            print(f"    MPN-like: {profile.get('regex_hits', {}).get('mpn_like', 0):.2%}")
    
    # Test normalization with profiling
    normalized = normalizer.normalize_with_profiling(test_data)
    
    print(f"\n{'='*60}")
    print("Normalized Rows:")
    print(f"{'='*60}")
    for i, row in enumerate(normalized, 1):
        print(f"\nRow {i}:")
        for key, value in row.items():
            if value:
                print(f"  {key}: {value}")
    
    print(f"\n{'='*60}")
    print("✓ Ambiguous column matching tests completed!")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    test_column_profiling()
    test_ambiguous_column_matching()
    
    print("\n" + "="*60)
    print("All column profiler tests completed!")
    print("="*60 + "\n")

