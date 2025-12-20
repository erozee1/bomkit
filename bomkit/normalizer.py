from typing import List, Dict, Any, Optional
import re
from .schema import STANDARD_HEADERS, COLUMN_MAPPINGS


class BomNormalizer:
    """Normalizer for standardizing Bill of Materials data.
    
    Maps various column name variations to a standard BOM template
    and normalizes the data structure.
    """
    
    def __init__(self):
        """Initialize the normalizer with column mappings."""
        # Create reverse lookup: normalized column name -> list of variations
        self._normalized_to_variations = {}
        for standard, variations in COLUMN_MAPPINGS.items():
            self._normalized_to_variations[standard] = variations
        
        # Create forward lookup: variation -> standard column name
        self._variation_to_standard = {}
        for standard, variations in COLUMN_MAPPINGS.items():
            for variation in variations:
                self._variation_to_standard[variation.lower()] = standard
    
    def get_standard_template(self) -> List[str]:
        """Get the standard BOM template headers.
        
        Returns:
            List of standard header names in order
        """
        return STANDARD_HEADERS.copy()
    
    def normalize_column_name(self, column_name: str) -> Optional[str]:
        """Normalize a column name to the standard header.
        
        Args:
            column_name: The original column name from the BOM
            
        Returns:
            Standard column name if a match is found, None otherwise
        """
        if not column_name:
            return None
        
        # Normalize the input: lowercase, strip whitespace, replace underscores/spaces
        normalized_input = re.sub(r'[\s_\-]+', ' ', column_name.lower().strip())
        
        # Direct lookup
        if normalized_input in self._variation_to_standard:
            return self._variation_to_standard[normalized_input]
        
        # Try exact match after normalization
        for variation, standard in self._variation_to_standard.items():
            if normalized_input == variation:
                return standard
        
        # Try partial matching (contains)
        for variation, standard in self._variation_to_standard.items():
            if variation in normalized_input or normalized_input in variation:
                return standard
        
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
        """Normalize reference designator string to comma-separated list format.
        
        Expands ranges to individual designators for more effective tracking.
        Each reference designator is explicitly listed, making it easier to track
        changes at the individual component level.
        
        Handles:
        - Ranges: "D1-D8" -> "D1, D2, D3, D4, D5, D6, D7, D8"
        - Comma-separated lists: "R1, R2, R3" -> "R1, R2, R3" (unchanged)
        - Mixed ranges and singles: "R1-R3, R5, R7-R9" -> "R1, R2, R3, R5, R7, R8, R9"
        - Non-consecutive: "C1, C2, C4" -> "C1, C2, C4" (unchanged)
        
        Args:
            ref_des: Reference designator string (e.g., "R1, R2, R3" or "D1-D8")
            
        Returns:
            Normalized reference designator string with all designators explicitly listed,
            separated by commas (e.g., "D1, D2, D3, D4, D5, D6, D7, D8")
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
                            # Expand the range to individual designators
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
        
        # Group by prefix and sort to maintain consistent ordering
        grouped = {}
        
        for prefix, num in parseable_designators:
            if prefix not in grouped:
                grouped[prefix] = []
            grouped[prefix].append(num)
        
        # Sort numbers for each prefix
        for prefix in grouped:
            grouped[prefix].sort()
        
        # Build normalized output - expand all to individual designators
        result_parts = []
        
        # Process each prefix group
        for prefix in sorted(grouped.keys()):
            numbers = grouped[prefix]
            if not numbers:
                continue
            
            # Add all individual designators (no range compression)
            for num in numbers:
                result_parts.append(f"{prefix}{num}")
        
        # Add unparseable items
        result_parts.extend(unparseable)
        
        return ', '.join(result_parts)
    
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
        
        return {
            "mapped": mapped,
            "unmapped": unmapped,
            "standard_headers": STANDARD_HEADERS
        }

