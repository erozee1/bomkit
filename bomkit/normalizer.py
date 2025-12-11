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
        
        return normalized_row
    
    def normalize(self, raw_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Normalize a list of raw rows to the standard template.
        
        Args:
            raw_rows: List of dictionaries representing BOM rows
            
        Returns:
            List of dictionaries with standard column names
        """
        return [self.normalize_row(row) for row in raw_rows]
    
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

