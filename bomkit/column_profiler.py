"""Column profiling module for instance/statistical similarity.

This module computes statistical profiles from sample column values to help
disambiguate ambiguous column names by analyzing the actual data content.
"""

import re
import statistics
from typing import Dict, List, Any, Optional, Tuple
from collections import Counter


class ColumnProfiler:
    """Profiles columns by analyzing sample values to infer column type and characteristics."""
    
    # Regex patterns for common BOM patterns
    MPN_PATTERNS = [
        r'^[A-Z0-9][A-Z0-9\-\._/]{2,63}$',  # Alphanumeric with separators
        r'^[A-Z]{2,6}\d{2,8}[A-Z0-9]*$',     # Common MPN format: letters + numbers
        r'^\d{1,3}[A-Z]{1,4}\d{1,6}$',       # Alternative: numbers + letters + numbers
    ]
    
    REF_DES_PATTERNS = [
        r'^[A-Z]\d+$',                        # Single: R1, C3, U1
        r'^[A-Z]\d+-[A-Z]\d+$',               # Range: R1-R3
        r'^[A-Z]\d+(, [A-Z]\d+)+$',           # List: R1, R2, R3
        r'^[A-Z]\d+(-[A-Z]\d+)?(, [A-Z]\d+(-[A-Z]\d+)?)*$',  # Mixed ranges and singles
    ]
    
    # Unit patterns (common electrical/mechanical units in BOMs)
    UNIT_PATTERNS = [
        # Voltage
        (r'[Vv]', 'V'),
        (r'[Mm][Vv]', 'mV'),
        (r'[Kk][Vv]', 'kV'),
        # Current
        (r'[Aa](?![a-zA-Z])', 'A'),
        (r'[Mm][Aa]', 'mA'),
        (r'[Uuµ][Aa]', 'uA'),
        (r'[Nn][Aa]', 'nA'),
        # Capacitance
        (r'[Pp][Ff]', 'pF'),
        (r'[Nn][Ff]', 'nF'),
        (r'[Uuµ][Ff]', 'uF'),
        (r'[Mm][Ff]', 'mF'),
        (r'[Ff](?![a-zA-Z])', 'F'),
        # Resistance
        (r'[Rr](?![a-zA-Z])', 'R'),
        (r'[Kk][Rr]', 'kR'),
        (r'[Mm][Rr]', 'MR'),
        (r'[Mm]Ω', 'mΩ'),
        (r'Ω', 'Ω'),
        (r'[Kk]Ω', 'kΩ'),
        (r'[Mm]Ω', 'MΩ'),
        (r'[Oo][Hh][Mm]', 'ohm'),
        # Length
        (r'[Mm][Mm](?![a-zA-Z])', 'mm'),
        (r'[Cc][Mm](?![a-zA-Z])', 'cm'),
        (r'[Mm](?![a-zA-Z])', 'm'),
        (r'[Kk][Mm]', 'km'),
        # Frequency
        (r'[Hh][Zz]', 'Hz'),
        (r'[Kk][Hh][Zz]', 'kHz'),
        (r'[Mm][Hh][Zz]', 'MHz'),
        (r'[Gg][Hh][Zz]', 'GHz'),
        # Power
        (r'[Ww](?![a-zA-Z])', 'W'),
        (r'[Mm][Ww]', 'mW'),
        (r'[Kk][Ww]', 'kW'),
    ]
    
    def __init__(self, sample_size: int = 200):
        """Initialize the column profiler.
        
        Args:
            sample_size: Maximum number of non-null values to sample (default: 200)
        """
        self.sample_size = sample_size
    
    def profile_column(self, column_name: str, values: List[Any]) -> Dict[str, Any]:
        """Compute a statistical profile for a column.
        
        Args:
            column_name: Name of the column being profiled
            values: List of all values in the column
            
        Returns:
            Dictionary containing the column profile with statistics
        """
        # Get sample of non-null values
        sample = self._get_sample(values)
        
        if not sample:
            return {
                'column_name': column_name,
                'sample_size': 0,
                'null_count': len([v for v in values if v is None or v == '']),
                'error': 'No non-null values found'
            }
        
        # Convert all values to strings for analysis
        sample_str = [str(v).strip() for v in sample if v is not None and str(v).strip()]
        
        if not sample_str:
            return {
                'column_name': column_name,
                'sample_size': 0,
                'null_count': len([v for v in values if v is None or v == '']),
                'error': 'No valid string values found'
            }
        
        profile = {
            'column_name': column_name,
            'sample_size': len(sample_str),
            'null_count': len([v for v in values if v is None or v == '']),
            'type_distribution': self._infer_type_distribution(sample_str),
            'regex_hits': self._check_regex_patterns(sample_str),
            'unit_presence': self._detect_units(sample_str),
            'cardinality': self._compute_cardinality(sample_str),
            'length_stats': self._compute_length_stats(sample_str),
            'character_class_stats': self._compute_character_class_stats(sample_str),
        }
        
        return profile
    
    def _get_sample(self, values: List[Any]) -> List[Any]:
        """Get a sample of non-null values from the column.
        
        Args:
            values: List of all values
            
        Returns:
            List of up to sample_size non-null values
        """
        non_null = [v for v in values if v is not None and v != '']
        return non_null[:self.sample_size]
    
    def _infer_type_distribution(self, values: List[str]) -> Dict[str, float]:
        """Infer type distribution: numeric vs text vs mixed.
        
        Args:
            values: List of string values
            
        Returns:
            Dictionary with 'numeric', 'text', and 'mixed' ratios
        """
        numeric_count = 0
        text_count = 0
        
        for value in values:
            # Try to parse as number (int or float)
            try:
                float(value)
                numeric_count += 1
            except (ValueError, TypeError):
                text_count += 1
        
        total = len(values)
        if total == 0:
            return {'numeric': 0.0, 'text': 0.0, 'mixed': 0.0}
        
        numeric_ratio = numeric_count / total
        text_ratio = text_count / total
        
        # Determine if mixed (both types present)
        mixed_ratio = 1.0 if (numeric_count > 0 and text_count > 0) else 0.0
        
        return {
            'numeric': numeric_ratio,
            'text': text_ratio,
            'mixed': mixed_ratio
        }
    
    def _check_regex_patterns(self, values: List[str]) -> Dict[str, float]:
        """Check for regex pattern matches (MPN, reference designators).
        
        Args:
            values: List of string values
            
        Returns:
            Dictionary with match ratios for each pattern type
        """
        mpn_matches = 0
        ref_des_matches = 0
        
        for value in values:
            # Check MPN patterns
            for pattern in self.MPN_PATTERNS:
                if re.match(pattern, value):
                    mpn_matches += 1
                    break
            
            # Check reference designator patterns
            for pattern in self.REF_DES_PATTERNS:
                if re.match(pattern, value):
                    ref_des_matches += 1
                    break
        
        total = len(values)
        if total == 0:
            return {'mpn_like': 0.0, 'ref_des_like': 0.0}
        
        return {
            'mpn_like': mpn_matches / total,
            'ref_des_like': ref_des_matches / total
        }
    
    def _detect_units(self, values: List[str]) -> Dict[str, float]:
        """Detect presence of units in values.
        
        Args:
            values: List of string values
            
        Returns:
            Dictionary with unit detection ratios for common units
        """
        unit_counts = Counter()
        
        for value in values:
            # Check each unit pattern
            for pattern, unit_name in self.UNIT_PATTERNS:
                if re.search(pattern, value):
                    unit_counts[unit_name] += 1
                    break  # Count each value only once
        
        total = len(values)
        if total == 0:
            return {}
        
        # Return ratios for each detected unit
        unit_presence = {}
        for unit_name in unit_counts:
            unit_presence[unit_name] = unit_counts[unit_name] / total
        
        return unit_presence
    
    def _compute_cardinality(self, values: List[str]) -> Dict[str, float]:
        """Compute cardinality statistics: unique ratio, repeated ratio.
        
        Args:
            values: List of string values
            
        Returns:
            Dictionary with unique_ratio and repeated_ratio
        """
        if not values:
            return {'unique_ratio': 0.0, 'repeated_ratio': 0.0}
        
        value_counts = Counter(values)
        total = len(values)
        unique_count = len(value_counts)
        
        unique_ratio = unique_count / total
        
        # Count values that appear more than once
        repeated_count = sum(1 for count in value_counts.values() if count > 1)
        repeated_ratio = repeated_count / unique_count if unique_count > 0 else 0.0
        
        return {
            'unique_ratio': unique_ratio,
            'repeated_ratio': repeated_ratio,
            'unique_count': unique_count,
            'total_count': total
        }
    
    def _compute_length_stats(self, values: List[str]) -> Dict[str, float]:
        """Compute length statistics: mean, median, min, max.
        
        Args:
            values: List of string values
            
        Returns:
            Dictionary with length statistics
        """
        if not values:
            return {'mean': 0.0, 'median': 0.0, 'min': 0.0, 'max': 0.0}
        
        lengths = [len(v) for v in values]
        
        return {
            'mean': statistics.mean(lengths),
            'median': statistics.median(lengths),
            'min': min(lengths),
            'max': max(lengths)
        }
    
    def _compute_character_class_stats(self, values: List[str]) -> Dict[str, float]:
        """Compute character class statistics: percent digits, punctuation, letters.
        
        Args:
            values: List of string values
            
        Returns:
            Dictionary with character class percentages
        """
        if not values:
            return {
                'percent_digits': 0.0,
                'percent_punctuation': 0.0,
                'percent_letters': 0.0,
                'percent_whitespace': 0.0
            }
        
        total_chars = 0
        digit_count = 0
        punct_count = 0
        letter_count = 0
        whitespace_count = 0
        
        for value in values:
            for char in value:
                total_chars += 1
                if char.isdigit():
                    digit_count += 1
                elif char.isalpha():
                    letter_count += 1
                elif char.isspace():
                    whitespace_count += 1
                elif char in '.,;:!?-\'"/()[]{}_':
                    punct_count += 1
        
        if total_chars == 0:
            return {
                'percent_digits': 0.0,
                'percent_punctuation': 0.0,
                'percent_letters': 0.0,
                'percent_whitespace': 0.0
            }
        
        return {
            'percent_digits': (digit_count / total_chars) * 100,
            'percent_punctuation': (punct_count / total_chars) * 100,
            'percent_letters': (letter_count / total_chars) * 100,
            'percent_whitespace': (whitespace_count / total_chars) * 100
        }
    
    def profile_dataframe(self, data: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """Profile all columns in a dataset.
        
        Args:
            data: List of dictionaries representing rows
            
        Returns:
            Dictionary mapping column names to their profiles
        """
        if not data:
            return {}
        
        # Collect all column names
        all_columns = set()
        for row in data:
            all_columns.update(row.keys())
        
        # Extract values for each column
        column_values = {}
        for column in all_columns:
            column_values[column] = [row.get(column) for row in data]
        
        # Profile each column
        profiles = {}
        for column, values in column_values.items():
            profiles[column] = self.profile_column(column, values)
        
        return profiles
    
    def compare_profiles(self, profile1: Dict[str, Any], profile2: Dict[str, Any]) -> float:
        """Compare two column profiles and return a similarity score.
        
        This can be used to match ambiguous column names by comparing their
        statistical profiles.
        
        Args:
            profile1: First column profile
            profile2: Second column profile
            
        Returns:
            Similarity score between 0 and 1
        """
        # If either profile has an error, return 0
        if 'error' in profile1 or 'error' in profile2:
            return 0.0
        
        scores = []
        
        # Compare type distribution
        type1 = profile1.get('type_distribution', {})
        type2 = profile2.get('type_distribution', {})
        type_similarity = self._compare_dicts(type1, type2)
        scores.append(type_similarity)
        
        # Compare regex hits
        regex1 = profile1.get('regex_hits', {})
        regex2 = profile2.get('regex_hits', {})
        regex_similarity = self._compare_dicts(regex1, regex2)
        scores.append(regex_similarity)
        
        # Compare unit presence
        units1 = profile1.get('unit_presence', {})
        units2 = profile2.get('unit_presence', {})
        unit_similarity = self._compare_dicts(units1, units2)
        scores.append(unit_similarity)
        
        # Compare cardinality (unique ratio)
        card1 = profile1.get('cardinality', {})
        card2 = profile2.get('cardinality', {})
        if 'unique_ratio' in card1 and 'unique_ratio' in card2:
            card_diff = abs(card1['unique_ratio'] - card2['unique_ratio'])
            card_similarity = 1.0 - min(card_diff, 1.0)
            scores.append(card_similarity)
        
        # Compare length stats (mean)
        length1 = profile1.get('length_stats', {})
        length2 = profile2.get('length_stats', {})
        if 'mean' in length1 and 'mean' in length2:
            if length1['mean'] > 0 and length2['mean'] > 0:
                length_diff = abs(length1['mean'] - length2['mean']) / max(length1['mean'], length2['mean'])
                length_similarity = 1.0 - min(length_diff, 1.0)
                scores.append(length_similarity)
        
        # Compare character class stats
        char1 = profile1.get('character_class_stats', {})
        char2 = profile2.get('character_class_stats', {})
        char_similarity = self._compare_dicts(char1, char2)
        scores.append(char_similarity)
        
        if not scores:
            return 0.0
        
        # Return weighted average
        return sum(scores) / len(scores)
    
    def _compare_dicts(self, dict1: Dict[str, float], dict2: Dict[str, float]) -> float:
        """Compare two dictionaries of numeric values.
        
        Args:
            dict1: First dictionary
            dict2: Second dictionary
            
        Returns:
            Similarity score between 0 and 1
        """
        all_keys = set(dict1.keys()) | set(dict2.keys())
        if not all_keys:
            return 1.0
        
        similarities = []
        for key in all_keys:
            val1 = dict1.get(key, 0.0)
            val2 = dict2.get(key, 0.0)
            
            # Compute similarity for this key
            if val1 == 0 and val2 == 0:
                similarity = 1.0
            elif val1 == 0 or val2 == 0:
                similarity = 0.0
            else:
                # Use relative difference
                diff = abs(val1 - val2)
                max_val = max(abs(val1), abs(val2))
                similarity = 1.0 - min(diff / max_val, 1.0)
            
            similarities.append(similarity)
        
        return sum(similarities) / len(similarities) if similarities else 0.0




