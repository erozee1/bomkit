#!/usr/bin/env python3
"""Simple test script for lexical similarity functionality."""

import sys
import os

# Add the project root to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Test lexical similarity directly
from bomkit.lexical_similarity import LexicalSimilarity, should_use_lexical_similarity

def test_lexical_similarity():
    """Test basic lexical similarity functionality."""
    ls = LexicalSimilarity()
    
    print("Testing lexical similarity features...")
    
    # Test 1: Normalization
    print("\n1. Testing normalization:")
    test_cases = [
        ("Part_Number", "part number"),
        ("ManufacturerName", "manufacturer name"),
        ("qty.", "quantity"),
    ]
    for input_text, expected_pattern in test_cases:
        normalized = ls.normalize_text(input_text)
        print(f"   '{input_text}' -> '{normalized}'")
    
    # Test 2: Jaro-Winkler for short labels
    print("\n2. Testing Jaro-Winkler (short labels):")
    test_cases = [
        ("mpn", "mnp"),  # Typo
        ("qty", "qty"),
        ("mfg", "mfr"),
    ]
    for s1, s2 in test_cases:
        score = ls.jaro_winkler_similarity(s1, s2)
        print(f"   '{s1}' vs '{s2}': {score:.3f}")
    
    # Test 3: Token similarity
    print("\n3. Testing token similarity:")
    test_cases = [
        ("part description", "description"),
        ("manufacturer name", "manufacturer"),
        ("unit of measure", "unit"),
    ]
    for s1, s2 in test_cases:
        jaccard = ls.jaccard_similarity(s1, s2)
        cosine = ls.cosine_similarity(s1, s2)
        print(f"   '{s1}' vs '{s2}': Jaccard={jaccard:.3f}, Cosine={cosine:.3f}")
    
    # Test 4: Combined similarity
    print("\n4. Testing combined similarity:")
    test_cases = [
        ("quantitiy", "quantity"),  # Misspelling
        ("manufaturer", "manufacturer"),  # Misspelling
        ("part desc", "description"),  # Abbreviation
        ("qty", "quantity"),  # Abbreviation
    ]
    for s1, s2 in test_cases:
        score = ls.calculate_similarity(s1, s2)
        print(f"   '{s1}' vs '{s2}': {score:.3f}")
    
    # Test 5: Spell check
    print("\n5. Testing spell check:")
    test_words = ["quantitiy", "manufaturer", "descripton"]
    for word in test_words:
        corrected = ls.spell_check(word)
        print(f"   '{word}' -> '{corrected}'")
    
    # Test 6: Column type detection
    print("\n6. Testing column type detection:")
    test_columns = [
        "description",
        "part_number",
        "quantity",
        "manufacturer_part_number",
        "manufacturer",
    ]
    for col in test_columns:
        should_use = should_use_lexical_similarity(col)
        print(f"   '{col}': use_lexical={should_use}")
    
    print("\n✓ All tests completed!")

if __name__ == "__main__":
    test_lexical_similarity()

