from typing import List, Dict, Any, Optional
import re
from .schema import STANDARD_HEADERS, COLUMN_MAPPINGS
from .lexical_similarity import LexicalSimilarity, should_use_lexical_similarity, LEXICAL_COLUMNS
from .column_profiler import ColumnProfiler


class BomNormalizer:
    """Normalizer for standardizing Bill of Materials data.
    
    Maps various column name variations to a standard BOM template
    and normalizes the data structure.
    """
    
    def __init__(self, use_lexical_similarity: bool = True, similarity_threshold: float = 0.6,
                 use_column_profiling: bool = True, profile_similarity_threshold: float = 0.7):
        """Initialize the normalizer with column mappings.
        
        Args:
            use_lexical_similarity: If True, use lexical similarity for matching (default: True)
            similarity_threshold: Minimum similarity score for lexical matching (default: 0.6)
            use_column_profiling: If True, use column profiling for ambiguous columns (default: True)
            profile_similarity_threshold: Minimum profile similarity score for matching (default: 0.7)
        """
        # Create reverse lookup: normalized column name -> list of variations
        self._normalized_to_variations = {}
        for standard, variations in COLUMN_MAPPINGS.items():
            self._normalized_to_variations[standard] = variations
        
        # Create forward lookup: variation -> standard column name
        self._variation_to_standard = {}
        for standard, variations in COLUMN_MAPPINGS.items():
            for variation in variations:
                self._variation_to_standard[variation.lower()] = standard
        
        # Initialize lexical similarity if enabled
        self.use_lexical_similarity = use_lexical_similarity
        self.similarity_threshold = similarity_threshold
        self.lexical_similarity = LexicalSimilarity() if use_lexical_similarity else None
        
        # Initialize column profiler if enabled
        self.use_column_profiling = use_column_profiling
        self.profile_similarity_threshold = profile_similarity_threshold
        self.column_profiler = ColumnProfiler() if use_column_profiling else None
        self._column_profiles_cache = {}  # Cache profiles for reuse
    
    def get_standard_template(self) -> List[str]:
        """Get the standard BOM template headers.
        
        Returns:
            List of standard header names in order
        """
        return STANDARD_HEADERS.copy()
    
    def normalize_column_name(self, column_name: str) -> Optional[str]:
        """Normalize a column name to the standard header.
        
        Uses lexical similarity for headers and relevant columns (description, quantity,
        unit, manufacturer, notes) but not for non-natural language columns like
        part numbers.
        
        Args:
            column_name: The original column name from the BOM
            
        Returns:
            Standard column name if a match is found, None otherwise
        """
        if not column_name:
            return None
        
        # Normalize the input: lowercase, strip whitespace, replace underscores/spaces
        normalized_input = re.sub(r'[\s_\-]+', ' ', column_name.lower().strip())
        
        # Direct lookup (exact match)
        if normalized_input in self._variation_to_standard:
            return self._variation_to_standard[normalized_input]
        
        # Try exact match after normalization (redundant but safe)
        for variation, standard in self._variation_to_standard.items():
            if normalized_input == variation:
                return standard
        
        # Check if this is a non-lexical column first (part numbers, etc.)
        # These should use strict matching only, not partial or lexical
        is_non_lexical = not should_use_lexical_similarity(column_name)
        
        # For non-lexical columns, try strict partial matching only
        # (exact word boundaries, not substring)
        if is_non_lexical:
            # Try matching as whole words only
            normalized_words = set(normalized_input.split())
            for variation, standard in self._variation_to_standard.items():
                variation_words = set(variation.split())
                # Only match if all words match (strict)
                if normalized_words == variation_words:
                    return standard
            # Non-lexical columns don't use fuzzy matching
            return None
        
        # For lexical columns, try partial matching (more lenient)
        # But be careful - prefer longer, more specific matches
        partial_matches = []
        for variation, standard in self._variation_to_standard.items():
            if variation in normalized_input or normalized_input in variation:
                # Prefer longer matches (more specific)
                partial_matches.append((len(variation), standard, variation))
        
        if partial_matches:
            # Sort by length (longest first) and return the best match
            partial_matches.sort(reverse=True)
            return partial_matches[0][1]
        
        # Use lexical similarity for headers and relevant columns only
        if self.use_lexical_similarity and self.lexical_similarity:
            # Check if this column should use lexical similarity
            if should_use_lexical_similarity(column_name):
                # Try spell-check first for common typos
                corrected = self.lexical_similarity.spell_check(column_name)
                if corrected:
                    # Try matching the corrected version
                    corrected_normalized = re.sub(r'[\s_\-]+', ' ', corrected.lower().strip())
                    if corrected_normalized in self._variation_to_standard:
                        return self._variation_to_standard[corrected_normalized]
                
                # Try lexical similarity matching against known variations
                # Group candidates by standard column
                candidates_by_standard = {}
                for standard, variations in COLUMN_MAPPINGS.items():
                    if standard in LEXICAL_COLUMNS:
                        candidates_by_standard[standard] = variations
                
                # Find best match using lexical similarity
                best_match = None
                best_score = 0.0
                
                for standard, variations in candidates_by_standard.items():
                    match_result = self.lexical_similarity.find_best_match(
                        column_name, variations, threshold=self.similarity_threshold
                    )
                    if match_result:
                        candidate, score = match_result
                        if score > best_score:
                            best_score = score
                            best_match = standard
                
                if best_match:
                    return best_match
        
        return None
    
    def normalize_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a single row to the standard template.
        
        Args:
            row: Dictionary representing a single BOM row
            
        Returns:
            Dictionary with standard column names, missing columns set to empty string
        """
        normalized_row = {}
        
        # Initialize all standard headers with empty strings
        for header in STANDARD_HEADERS:
            normalized_row[header] = ""
        
        # Map original columns to standard columns
        for original_key, value in row.items():
            if original_key is None:
                continue
            
            standard_key = self.normalize_column_name(str(original_key))
            if standard_key:
                # Handle multiple values (e.g., if multiple columns map to same standard)
                if normalized_row[standard_key]:
                    # Append if already has value (for reference_designator, notes, etc.)
                    if standard_key in ["reference_designator", "notes"]:
                        normalized_row[standard_key] = f"{normalized_row[standard_key]}, {value}"
                    else:
                        # Keep first non-empty value for other fields
                        if not normalized_row[standard_key]:
                            normalized_row[standard_key] = str(value) if value is not None else ""
                else:
                    normalized_row[standard_key] = str(value) if value is not None else ""
            else:
                # Unmapped columns go to notes
                if value:
                    if normalized_row["notes"]:
                        normalized_row["notes"] = f"{normalized_row['notes']}; {original_key}: {value}"
                    else:
                        normalized_row["notes"] = f"{original_key}: {value}"
        
        # Clean up values: strip whitespace, handle empty strings
        for key, value in normalized_row.items():
            if isinstance(value, str):
                normalized_row[key] = value.strip()
            elif value is None:
                normalized_row[key] = ""
        
        # Normalize reference designator ranges
        if normalized_row.get("reference_designator"):
            normalized_row["reference_designator"] = self.normalize_reference_designator(
                normalized_row["reference_designator"]
            )
        
        return normalized_row
    
    def normalize(self, raw_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Normalize a list of raw rows to the standard template.
        
        Args:
            raw_rows: List of dictionaries representing BOM rows
            
        Returns:
            List of dictionaries with standard column names
        """
        return [self.normalize_row(row) for row in raw_rows]
    
    def normalize_reference_designator(self, ref_des: str) -> str:
        """Normalize reference designator string to consistent format with ranges.
        
        Handles:
        - Comma-separated lists: "R1, R2, R3" -> "R1-R3"
        - Already formatted ranges: "R1-R3" -> "R1-R3" (unchanged)
        - Mixed ranges and singles: "R1-R3, R5, R7-R9" -> "R1-R3, R5, R7-R9"
        - Non-consecutive: "C1, C2, C4" -> "C1, C2, C4" or "C1-C2, C4"
        
        Args:
            ref_des: Reference designator string (e.g., "R1, R2, R3" or "D1-D8")
            
        Returns:
            Normalized reference designator string with consistent range formatting
        """
        if not ref_des or not ref_des.strip():
            return ""
        
        # Clean up the input
        ref_des = ref_des.strip()
        
        # Remove trailing commas and clean up whitespace
        ref_des = re.sub(r',\s*$', '', ref_des)  # Remove trailing comma
        ref_des = re.sub(r'\s*,\s*', ', ', ref_des)  # Normalize comma spacing
        
        # Split by comma to get individual designators
        parts = [p.strip() for p in ref_des.split(',') if p.strip()]
        
        if not parts:
            return ""
        
        # Parse designators into (prefix, number) tuples or keep unparseable as strings
        parseable_designators = []  # List of (prefix, number) tuples
        unparseable = []  # List of strings that couldn't be parsed
        
        for part in parts:
            part = part.strip()
            if not part:
                continue
            
            # Check if it's already a range (e.g., "R1-R5")
            if '-' in part and not part.startswith('-'):
                range_parts = part.split('-', 1)
                if len(range_parts) == 2:
                    start = range_parts[0].strip()
                    end = range_parts[1].strip()
                    # Parse both ends
                    start_match = re.match(r'^([A-Za-z]+)(\d+)$', start)
                    end_match = re.match(r'^([A-Za-z]+)(\d+)$', end)
                    if start_match and end_match:
                        start_prefix, start_num = start_match.groups()
                        end_prefix, end_num = end_match.groups()
                        if start_prefix == end_prefix:
                            # Expand the range
                            for num in range(int(start_num), int(end_num) + 1):
                                parseable_designators.append((start_prefix, num))
                        else:
                            # Different prefixes, treat as separate
                            parseable_designators.append((start_prefix, int(start_num)))
                            parseable_designators.append((end_prefix, int(end_num)))
                    else:
                        # Can't parse, keep as-is
                        unparseable.append(part)
                continue
            
            # Parse individual designator (e.g., "R1", "C42")
            match = re.match(r'^([A-Za-z]+)(\d+)$', part)
            if match:
                prefix, number = match.groups()
                parseable_designators.append((prefix, int(number)))
            else:
                # Can't parse, keep as-is
                unparseable.append(part)
        
        if not parseable_designators and not unparseable:
            return ref_des  # Return original if we can't parse anything
        
        # Group by prefix and sort
        grouped = {}
        
        for prefix, num in parseable_designators:
            if prefix not in grouped:
                grouped[prefix] = []
            grouped[prefix].append(num)
        
        # Sort numbers for each prefix
        for prefix in grouped:
            grouped[prefix].sort()
        
        # Build normalized output
        result_parts = []
        
        # Process each prefix group
        for prefix in sorted(grouped.keys()):
            numbers = grouped[prefix]
            if not numbers:
                continue
            
            # Find consecutive ranges
            ranges = []
            start = numbers[0]
            end = numbers[0]
            
            for i in range(1, len(numbers)):
                if numbers[i] == end + 1:
                    # Consecutive, extend range
                    end = numbers[i]
                else:
                    # Gap found, save current range
                    if start == end:
                        ranges.append(f"{prefix}{start}")
                    else:
                        ranges.append(f"{prefix}{start}-{prefix}{end}")
                    start = numbers[i]
                    end = numbers[i]
            
            # Add final range
            if start == end:
                ranges.append(f"{prefix}{start}")
            else:
                ranges.append(f"{prefix}{start}-{prefix}{end}")
            
            result_parts.extend(ranges)
        
        # Add unparseable items
        result_parts.extend(unparseable)
        
        return ', '.join(result_parts)
    
    def normalize_with_profiling(self, raw_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Normalize rows using column profiling for ambiguous columns.
        
        This method uses instance/statistical similarity to disambiguate columns
        when lexical matching is ambiguous (e.g., "Value" could be capacitance,
        cost, or "declared value").
        
        Args:
            raw_rows: List of dictionaries representing BOM rows
            
        Returns:
            List of dictionaries with standard column names
        """
        if not self.use_column_profiling or not self.column_profiler:
            # Fall back to standard normalization
            return self.normalize(raw_rows)
        
        # Profile all columns in the data
        column_profiles = self.column_profiler.profile_dataframe(raw_rows)
        self._column_profiles_cache = column_profiles
        
        # Get all column names
        all_columns = set()
        for row in raw_rows:
            all_columns.update(row.keys())
        
        # Build mapping using profiling for ambiguous columns
        column_mapping = {}
        ambiguous_columns = []
        
        for column in all_columns:
            if column is None:
                continue
            
            # First try standard normalization
            standard = self.normalize_column_name(str(column))
            
            if standard:
                column_mapping[column] = standard
            else:
                # Column is unmapped - check if it's ambiguous
                ambiguous_columns.append(column)
        
        # For ambiguous columns, use profiling to match
        if ambiguous_columns:
            # Get reference profiles for standard columns (if we have sample data)
            # For now, we'll use heuristics based on profile characteristics
            for column in ambiguous_columns:
                profile = column_profiles.get(column)
                if not profile or 'error' in profile:
                    continue
                
                # Try to match based on profile characteristics
                best_match = self._match_column_by_profile(column, profile, column_profiles)
                if best_match:
                    column_mapping[column] = best_match
        
        # Apply mapping to rows
        normalized_rows = []
        for row in raw_rows:
            normalized_row = {}
            
            # Initialize all standard headers with empty strings
            for header in STANDARD_HEADERS:
                normalized_row[header] = ""
            
            # Map original columns to standard columns
            for original_key, value in row.items():
                if original_key is None:
                    continue
                
                standard_key = column_mapping.get(str(original_key))
                if standard_key:
                    # Handle multiple values
                    if normalized_row[standard_key]:
                        if standard_key in ["reference_designator", "notes"]:
                            normalized_row[standard_key] = f"{normalized_row[standard_key]}, {value}"
                        else:
                            if not normalized_row[standard_key]:
                                normalized_row[standard_key] = str(value) if value is not None else ""
                    else:
                        normalized_row[standard_key] = str(value) if value is not None else ""
                else:
                    # Unmapped columns go to notes
                    if value:
                        if normalized_row["notes"]:
                            normalized_row["notes"] = f"{normalized_row['notes']}; {original_key}: {value}"
                        else:
                            normalized_row["notes"] = f"{original_key}: {value}"
            
            # Clean up values
            for key, value in normalized_row.items():
                if isinstance(value, str):
                    normalized_row[key] = value.strip()
                elif value is None:
                    normalized_row[key] = ""
            
            # Normalize reference designator ranges
            if normalized_row.get("reference_designator"):
                normalized_row["reference_designator"] = self.normalize_reference_designator(
                    normalized_row["reference_designator"]
                )
            
            normalized_rows.append(normalized_row)
        
        return normalized_rows
    
    def _match_column_by_profile(self, column_name: str, profile: Dict[str, Any], 
                                 all_profiles: Dict[str, Dict[str, Any]]) -> Optional[str]:
        """Match a column to a standard column based on its profile.
        
        Uses heuristics based on profile characteristics to match ambiguous columns.
        
        Args:
            column_name: Name of the column to match
            profile: Profile of the column
            all_profiles: All column profiles (for context)
            
        Returns:
            Standard column name if match found, None otherwise
        """
        # Heuristics for common ambiguous cases
        
        # "Value" column - could be electrical value, cost, or declared value
        if 'value' in column_name.lower():
            # Check if it looks like electrical value (has units, numeric with units)
            unit_presence = profile.get('unit_presence', {})
            type_dist = profile.get('type_distribution', {})
            
            if unit_presence and any(unit in ['V', 'A', 'uF', 'nF', 'pF', 'kΩ', 'Ω', 'R'] for unit in unit_presence):
                return 'value'  # Electrical value
            elif type_dist.get('numeric', 0) > 0.8 and not unit_presence:
                # Mostly numeric without units - could be cost or quantity
                # Check length - cost values are usually shorter
                length_stats = profile.get('length_stats', {})
                if length_stats.get('mean', 0) < 10:
                    # Could be cost, but we'll default to value
                    pass
            return 'value'  # Default to value
        
        # "Part" column - could be part_number, manufacturer_part_number, or description
        if 'part' in column_name.lower() and 'number' not in column_name.lower():
            regex_hits = profile.get('regex_hits', {})
            type_dist = profile.get('type_distribution', {})
            
            # If it looks like MPN (alphanumeric pattern)
            if regex_hits.get('mpn_like', 0) > 0.7:
                return 'manufacturer_part_number'
            # If it's mostly text and long, could be description
            elif type_dist.get('text', 0) > 0.7:
                length_stats = profile.get('length_stats', {})
                if length_stats.get('mean', 0) > 20:
                    return 'description'
            # Default to part_number
            return 'part_number'
        
        # Check for reference designator patterns
        regex_hits = profile.get('regex_hits', {})
        if regex_hits.get('ref_des_like', 0) > 0.7:
            return 'reference_designator'
        
        # Check for MPN patterns
        if regex_hits.get('mpn_like', 0) > 0.7:
            return 'manufacturer_part_number'
        
        # Check for units (electrical values)
        unit_presence = profile.get('unit_presence', {})
        if unit_presence and any(unit in ['V', 'A', 'uF', 'nF', 'pF', 'kΩ', 'Ω', 'R', 'Hz'] for unit in unit_presence):
            return 'value'
        
        return None
    
    def get_column_profiles(self, raw_rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """Get column profiles for all columns in the data.
        
        Args:
            raw_rows: List of dictionaries representing BOM rows
            
        Returns:
            Dictionary mapping column names to their profiles
        """
        if not self.use_column_profiling or not self.column_profiler:
            return {}
        
        return self.column_profiler.profile_dataframe(raw_rows)
    
    def get_mapping_report(self, raw_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate a report of column mappings for debugging.
        
        Args:
            raw_rows: List of dictionaries representing BOM rows
            
        Returns:
            Dictionary with mapping information
        """
        if not raw_rows:
            return {"mapped": {}, "unmapped": []}
        
        # Get all unique column names from raw rows
        all_columns = set()
        for row in raw_rows:
            all_columns.update(row.keys())
        
        mapped = {}
        unmapped = []
        
        for column in all_columns:
            if column is None:
                continue
            standard = self.normalize_column_name(str(column))
            if standard:
                if standard not in mapped:
                    mapped[standard] = []
                mapped[standard].append(column)
            else:
                unmapped.append(column)
        
        # Add profiling information if available
        report = {
            "mapped": mapped,
            "unmapped": unmapped,
            "standard_headers": STANDARD_HEADERS
        }
        
        if self.use_column_profiling and self.column_profiler:
            report["profiles"] = self.get_column_profiles(raw_rows)
        
        return report

