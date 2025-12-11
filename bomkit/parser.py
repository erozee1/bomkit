from .normalizer import BomNormalizer
from typing import List, Dict, Any, Optional


class BomParser:
    """Parser for Bill of Materials files with normalization support."""
    
    def __init__(self, normalize: bool = True):
        """Initialize the BOM parser.
        
        Args:
            normalize: If True, normalize rows to standard template (default: True)
        """
        self.adapters = []
        self.normalizer = BomNormalizer() if normalize else None
    
    def register_adapter(self, adapter):
        """Register a file adapter for parsing.
        
        Args:
            adapter: Adapter instance with can_handle() and read() methods
        """
        self.adapters.append(adapter)

    def parse(self, file_path: str, normalize: Optional[bool] = None) -> List[Dict[str, Any]]:
        """Parse a BOM file and optionally normalize to standard template.
        
        Args:
            file_path: Path to the BOM file
            normalize: Override default normalization setting (optional)
            
        Returns:
            List of dictionaries representing BOM rows.
            If normalization is enabled, rows use standard column names.
            Otherwise, returns raw rows with original column names.
            
        Raises:
            ValueError: If no adapter is found for the file
        """
        # Find appropriate adapter
        adapter = None
        for a in self.adapters:
            if a.can_handle(file_path):
                adapter = a
                break
        
        if adapter is None:
            raise ValueError(f"No adapter found for {file_path}")
        
        # Read raw rows
        raw_rows = adapter.read(file_path)
        
        # Normalize if requested
        should_normalize = normalize if normalize is not None else (self.normalizer is not None)
        if should_normalize and self.normalizer:
            return self.normalizer.normalize(raw_rows)
        
        return raw_rows
    
    def get_standard_template(self) -> List[str]:
        """Get the standard BOM template headers.
        
        Returns:
            List of standard header names
        """
        if self.normalizer:
            return self.normalizer.get_standard_template()
        return []
    
    def get_mapping_report(self, file_path: str) -> Dict[str, Any]:
        """Get a report of how columns from a file map to standard headers.
        
        Args:
            file_path: Path to the BOM file
            
        Returns:
            Dictionary with mapping information including mapped and unmapped columns
        """
        if not self.normalizer:
            return {"error": "Normalizer not initialized"}
        
        # Parse without normalization to get raw rows
        adapter = None
        for a in self.adapters:
            if a.can_handle(file_path):
                adapter = a
                break
        
        if adapter is None:
            raise ValueError(f"No adapter found for {file_path}")
        
        raw_rows = adapter.read(file_path)
        return self.normalizer.get_mapping_report(raw_rows)