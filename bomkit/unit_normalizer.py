"""Unit normalization using Pint library for converting values to SI engineering units."""

import re
from typing import Any, Dict, List, Optional, Tuple
from pint import UnitRegistry

# Initialize Pint unit registry
ureg = UnitRegistry()


class UnitNormalizer:
    """Normalizes numerical values with units to standard SI engineering units."""
    
    # Common unit patterns that might appear in BOM values
    # These patterns help identify values that need unit conversion
    UNIT_PATTERNS = [
        # Capacitance: nF, uF, mF, F, pF
        (r'([\d.]+)\s*(pF|nF|uF|µF|mF|F)', 'capacitance'),
        # Resistance: R, kR, MR, mΩ, Ω, kΩ, MΩ
        (r'([\d.]+)\s*(R|kR|MR|mΩ|Ω|kΩ|MΩ|ohm|kohm|Mohm)', 'resistance'),
        # Voltage: V, mV, kV, MV
        (r'([\d.]+)\s*(V|mV|kV|MV)', 'voltage'),
        # Current: A, mA, uA, µA, nA, pA
        (r'([\d.]+)\s*(A|mA|uA|µA|nA|pA)', 'current'),
        # Frequency: Hz, kHz, MHz, GHz
        (r'([\d.]+)\s*(Hz|kHz|MHz|GHz)', 'frequency'),
        # Power: W, mW, kW, MW
        (r'([\d.]+)\s*(W|mW|kW|MW)', 'power'),
        # Length: m, mm, cm, km, um, µm, nm
        (r'([\d.]+)\s*(m|mm|cm|km|um|µm|nm)', 'length'),
        # Mass: g, kg, mg, ug, µg
        (r'([\d.]+)\s*(g|kg|mg|ug|µg)', 'mass'),
        # Time: s, ms, us, µs, ns, ps
        (r'([\d.]+)\s*(s|ms|us|µs|ns|ps)', 'time'),
        # Common engineering notation (e.g., 10n, 100k, 1.5u)
        (r'([\d.]+)\s*([pnumkMG])(?![a-zA-Z])', 'engineering'),
    ]
    
    def __init__(self):
        """Initialize the unit normalizer."""
        self.ureg = ureg
    
    def normalize_element(self, value: Any) -> Tuple[Any, Optional[str], Optional[str]]:
        """Normalize a single element (value with optional unit) to SI base units.
        
        Args:
            value: The value to normalize (can be string, int, float, etc.)
            
        Returns:
            Tuple of (normalized_value, original_unit, normalized_unit)
            - normalized_value: The value converted to SI base units (float or original if no unit)
            - original_unit: The original unit string if found, None otherwise
            - normalized_unit: The SI base unit string if converted, None otherwise
            
        Examples:
            normalize_element("10nF") -> (1e-8, "nF", "F")
            normalize_element("100kΩ") -> (100000.0, "kΩ", "ohm")
            normalize_element("1.5V") -> (1.5, "V", "V")
            normalize_element("10n") -> (1e-8, "n", "F")  # Assumes capacitance if ambiguous
            normalize_element("42") -> (42, None, None)  # No unit detected
        """
        if value is None:
            return value, None, None
        
        # Convert to string for pattern matching
        value_str = str(value).strip()
        
        if not value_str:
            return value, None, None
        
        # Try to parse as a number first (no unit)
        try:
            # If it's just a number, return as-is
            float_val = float(value_str)
            # Check if it's an integer
            if float_val.is_integer():
                return int(float_val), None, None
            return float_val, None, None
        except (ValueError, TypeError):
            pass
        
        # Try to match unit patterns
        for pattern, unit_type in self.UNIT_PATTERNS:
            match = re.search(pattern, value_str, re.IGNORECASE)
            if match:
                num_str = match.group(1)
                unit_str = match.group(2)
                
                try:
                    num_value = float(num_str)
                    
                    # Handle engineering notation (e.g., 10n, 100k)
                    if unit_type == 'engineering':
                        normalized_value, normalized_unit = self._normalize_engineering_notation(
                            num_value, unit_str, value_str
                        )
                        if normalized_value is not None:
                            return normalized_value, unit_str, normalized_unit
                    
                    # Handle specific unit types
                    normalized_value, normalized_unit = self._normalize_with_pint(
                        num_value, unit_str, unit_type
                    )
                    
                    if normalized_value is not None:
                        return normalized_value, unit_str, normalized_unit
                        
                except (ValueError, TypeError):
                    continue
        
        # If no pattern matched, try direct Pint parsing
        try:
            quantity = self.ureg.parse_expression(value_str)
            # Convert to base units
            base_quantity = quantity.to_base_units()
            normalized_value = base_quantity.magnitude
            normalized_unit = str(base_quantity.units)
            return normalized_value, value_str, normalized_unit
        except Exception:
            # If all parsing fails, return original value
            return value, None, None
    
    def _normalize_engineering_notation(
        self, num_value: float, unit_str: str, original_str: str
    ) -> Tuple[Optional[float], Optional[str]]:
        """Normalize engineering notation (e.g., 10n, 100k) to SI units.
        
        Engineering notation is ambiguous - we need context. Common assumptions:
        - In BOMs, 'n', 'u', 'p' often refer to capacitance (nF, uF, pF)
        - 'k', 'M' often refer to resistance (kΩ, MΩ) or capacitance
        - We'll try common interpretations
        
        Args:
            num_value: The numeric value
            unit_str: The unit prefix (p, n, u, m, k, M, G)
            original_str: The original string for context
            
        Returns:
            Tuple of (normalized_value, normalized_unit) or (None, None) if can't determine
        """
        prefix_map = {
            'p': 1e-12,  # pico
            'n': 1e-9,   # nano
            'u': 1e-6,   # micro
            'µ': 1e-6,   # micro (unicode)
            'm': 1e-3,   # milli
            'k': 1e3,    # kilo
            'M': 1e6,    # mega
            'G': 1e9,    # giga
        }
        
        unit_str_lower = unit_str.lower()
        if unit_str_lower not in prefix_map:
            return None, None
        
        multiplier = prefix_map[unit_str_lower]
        
        # Try to infer unit type from context
        # Common in BOMs: values like "10n" are often capacitors (nF)
        # Values like "100k" are often resistors (kΩ)
        # Values like "1.5u" could be capacitance (uF) or current (uA)
        
        # Heuristic: if value is small (< 1 with prefix), likely capacitance
        # If value is large (> 100 with prefix), likely resistance
        if unit_str_lower in ['p', 'n', 'u', 'µ']:
            # Likely capacitance
            normalized_value = num_value * multiplier  # Convert to Farads
            return normalized_value, 'F'
        elif unit_str_lower in ['k', 'M']:
            # Likely resistance (kΩ, MΩ)
            normalized_value = num_value * multiplier  # Convert to Ohms
            return normalized_value, 'ohm'
        else:
            # Default: assume base unit (no conversion)
            return num_value, None
    
    def _normalize_with_pint(
        self, num_value: float, unit_str: str, unit_type: str
    ) -> Tuple[Optional[float], Optional[str]]:
        """Normalize a value using Pint library.
        
        Args:
            num_value: The numeric value
            unit_str: The unit string
            unit_type: The type of unit (capacitance, resistance, etc.)
            
        Returns:
            Tuple of (normalized_value, normalized_unit) or (None, None) if conversion fails
        """
        try:
            # Map common unit abbreviations to Pint-compatible units
            # Pint uses standard unit names with prefixes
            unit_mapping = {
                # Capacitance - Pint uses 'farad' (lowercase)
                'pF': 'picofarad', 'nF': 'nanofarad', 'uF': 'microfarad',
                'µF': 'microfarad', 'mF': 'millifarad', 'F': 'farad',
                # Resistance - Pint uses 'ohm'
                'R': 'ohm', 'kR': 'kiloohm', 'MR': 'megaohm',
                'mΩ': 'milliohm', 'Ω': 'ohm', 'kΩ': 'kiloohm', 'MΩ': 'megaohm',
                'ohm': 'ohm', 'kohm': 'kiloohm', 'Mohm': 'megaohm',
                # Voltage - Pint uses 'volt'
                'V': 'volt', 'mV': 'millivolt', 'kV': 'kilovolt', 'MV': 'megavolt',
                # Current - Pint uses 'ampere'
                'A': 'ampere', 'mA': 'milliampere', 'uA': 'microampere',
                'µA': 'microampere', 'nA': 'nanoampere', 'pA': 'picoampere',
                # Frequency - Pint uses 'hertz'
                'Hz': 'hertz', 'kHz': 'kilohertz', 'MHz': 'megahertz', 'GHz': 'gigahertz',
                # Power - Pint uses 'watt'
                'W': 'watt', 'mW': 'milliwatt', 'kW': 'kilowatt', 'MW': 'megawatt',
                # Length - Pint uses 'meter'
                'm': 'meter', 'mm': 'millimeter', 'cm': 'centimeter',
                'km': 'kilometer', 'um': 'micrometer', 'µm': 'micrometer', 'nm': 'nanometer',
                # Mass - Pint uses 'gram' or 'kilogram'
                'g': 'gram', 'kg': 'kilogram', 'mg': 'milligram',
                'ug': 'microgram', 'µg': 'microgram',
                # Time - Pint uses 'second'
                's': 'second', 'ms': 'millisecond', 'us': 'microsecond',
                'µs': 'microsecond', 'ns': 'nanosecond', 'ps': 'picosecond',
            }
            
            # Get Pint-compatible unit name
            pint_unit = unit_mapping.get(unit_str, unit_str)
            
            # Try to create quantity using Pint
            # First try with mapped unit name
            try:
                quantity = num_value * getattr(self.ureg, pint_unit)
            except AttributeError:
                # If that fails, try direct parsing with original unit string
                try:
                    quantity = self.ureg.parse_expression(f"{num_value} {unit_str}")
                except Exception:
                    # If that also fails, try with mapped unit
                    quantity = self.ureg.parse_expression(f"{num_value} {pint_unit}")
            
            # Convert to base units
            base_quantity = quantity.to_base_units()
            normalized_value = base_quantity.magnitude
            normalized_unit = str(base_quantity.units)
            
            return normalized_value, normalized_unit
            
        except Exception:
            return None, None
    
    def normalize_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize all values in a row that contain units.
        
        Args:
            row: Dictionary representing a BOM row
            
        Returns:
            Dictionary with normalized values (original structure preserved)
        """
        normalized_row = {}
        
        for key, value in row.items():
            normalized_value, original_unit, normalized_unit = self.normalize_element(value)
            
            # Store normalized value
            # If unit was normalized, we could store both original and normalized
            # For now, we'll store the normalized value
            normalized_row[key] = normalized_value
            
            # Optionally, store unit information in a separate field
            # This could be useful for tracking what was converted
            if original_unit and normalized_unit:
                # Store metadata about the conversion (optional)
                # normalized_row[f"{key}_original"] = value
                # normalized_row[f"{key}_unit"] = normalized_unit
                pass
        
        return normalized_row
    
    def normalize_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Normalize all values in a list of rows.
        
        Args:
            data: List of dictionaries representing BOM rows
            
        Returns:
            List of dictionaries with normalized values
        """
        return [self.normalize_row(row) for row in data]

