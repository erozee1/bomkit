import csv
import chardet
from pathlib import Path
from typing import List, Dict, Any


class CsvAdapter:
    """CSV adapter for reading CSV and TSV files reliably.
    
    Handles:
    - Multiple encodings (UTF-8, UTF-8-BOM, Windows-1252, ISO-8859-1, etc.)
    - Different delimiters (comma, semicolon, tab)
    - Edge cases (empty files, missing headers, malformed rows)
    """
    
    def can_handle(self, file_path: str) -> bool:
        """Check if this adapter can handle the given file."""
        return Path(file_path).suffix.lower() in [".csv", ".tsv"]
    
    def _detect_encoding(self, file_path: str) -> str:
        """Detect file encoding using chardet with fallback."""
        try:
            with open(file_path, 'rb') as f:
                raw_data = f.read(10000)  # Read first 10KB for detection
                
                # Check for BOM first
                if raw_data.startswith(b'\xef\xbb\xbf'):
                    return 'utf-8-sig'
                
                # Use chardet for detection
                result = chardet.detect(raw_data)
                encoding = result.get('encoding', 'utf-8')
                
                # Handle common encoding issues
                if encoding is None:
                    encoding = 'utf-8'
                
                # Normalize common encodings
                encoding_lower = encoding.lower()
                if 'utf-8' in encoding_lower or 'utf8' in encoding_lower:
                    return 'utf-8'
                
                return encoding
        except Exception:
            # Fallback to UTF-8 if detection fails
            return 'utf-8'
    
    def _detect_delimiter(self, file_path: str, encoding: str) -> str:
        """Detect CSV delimiter by analyzing the first line."""
        path = Path(file_path)
        suffix = path.suffix.lower()
        
        # TSV files use tab delimiter
        if suffix == '.tsv':
            return '\t'
        
        # For CSV, try to detect delimiter
        try:
            with open(file_path, 'r', encoding=encoding, newline='') as f:
                # Read first line
                first_line = f.readline()
                
                # Count occurrences of common delimiters
                comma_count = first_line.count(',')
                semicolon_count = first_line.count(';')
                tab_count = first_line.count('\t')
                
                # Return delimiter with highest count
                if tab_count > comma_count and tab_count > semicolon_count:
                    return '\t'
                elif semicolon_count > comma_count:
                    return ';'
                else:
                    return ','
        except Exception:
            # Default to comma if detection fails
            return ','
    
    def read(self, file_path: str) -> List[Dict[str, Any]]:
        """Read CSV file and return raw rows as list of dictionaries.
        
        Args:
            file_path: Path to the CSV/TSV file
            
        Returns:
            List of dictionaries, where each dictionary represents a row
            with column names as keys
            
        Raises:
            FileNotFoundError: If file doesn't exist
            ValueError: If file is empty or cannot be parsed
        """
        path = Path(file_path)
        
        # Check if file exists
        if not path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # Check if file is empty
        if path.stat().st_size == 0:
            return []
        
        # Detect encoding
        encoding = self._detect_encoding(file_path)
        
        # Detect delimiter
        delimiter = self._detect_delimiter(file_path, encoding)
        
        # Read CSV file
        rows = []
        try:
            with open(file_path, 'r', encoding=encoding, newline='') as f:
                # Use Sniffer for more robust delimiter detection if needed
                try:
                    sample = f.read(1024)
                    f.seek(0)
                    sniffer = csv.Sniffer()
                    dialect = sniffer.sniff(sample, delimiters=',;\t')
                    delimiter = dialect.delimiter
                except (csv.Error, Exception):
                    # Fall back to detected delimiter
                    pass
                
                reader = csv.DictReader(f, delimiter=delimiter)
                
                # Read all rows
                for row in reader:
                    # Convert all values to strings, handling None
                    cleaned_row = {
                        key: str(value) if value is not None else ''
                        for key, value in row.items()
                    }
                    rows.append(cleaned_row)
        
        except UnicodeDecodeError as e:
            # Try with different encoding as fallback
            fallback_encodings = ['latin-1', 'cp1252', 'iso-8859-1']
            for fallback_encoding in fallback_encodings:
                try:
                    with open(file_path, 'r', encoding=fallback_encoding, newline='') as f:
                        reader = csv.DictReader(f, delimiter=delimiter)
                        rows = [
                            {
                                key: str(value) if value is not None else ''
                                for key, value in row.items()
                            }
                            for row in reader
                        ]
                        break
                except (UnicodeDecodeError, Exception):
                    continue
            else:
                raise ValueError(f"Could not decode file {file_path}: {e}")
        
        except csv.Error as e:
            raise ValueError(f"Error parsing CSV file {file_path}: {e}")
        
        return rows