"""Lexical similarity module for header and column name matching.

This module provides multiple lexical similarity features for matching
column names and headers, with special handling for natural language
vs. non-natural language columns (e.g., part numbers).
"""

import re
from typing import Dict, List, Set, Tuple, Optional
from collections import Counter
import math


# Columns that should use lexical similarity (natural language)
LEXICAL_COLUMNS = {
    "description",
    "quantity",
    "unit",
    "manufacturer",
    "notes"
}

# Columns that should NOT use lexical similarity (non-natural language)
NON_LEXICAL_COLUMNS = {
    "part_number",
    "manufacturer_part_number",
    "reference_designator",
    "value",
    "package"
}

# Abbreviation dictionary for expansion
ABBREVIATION_DICT = {
    "mfr": "manufacturer",
    "mfg": "manufacturer",
    "qty": "quantity",
    "qnty": "quantity",
    "qty.": "quantity",
    "uom": "unit of measure",
    "mpn": "manufacturer part number",
    "pn": "part number",
    "p/n": "part number",
    "ref": "reference",
    "des": "designator",
    "refdes": "reference designator",
    "pkg": "package",
    "pkg.": "package",
    "spec": "specification",
    "specs": "specifications",
    "desc": "description",
    "desc.": "description",
    "comm": "comment",
    "comm.": "comment",
    "comms": "comments",
    "misc": "miscellaneous",
    "misc.": "miscellaneous",
    "info": "information",
    "info.": "information",
    "amt": "amount",
    "amt.": "amount",
    "cnt": "count",
    "cnt.": "count",
    "meas": "measure",
    "meas.": "measure",
    "measmt": "measurement",
    "measmt.": "measurement",
    "vend": "vendor",
    "vend.": "vendor",
    "supp": "supplier",
    "supp.": "supplier",
    "name": "name",
    "nm": "name",
    "nm.": "name",
}

# Common misspellings dictionary (engineer typos)
COMMON_MISSPELLINGS = {
    "manufaturer": "manufacturer",
    "manufacter": "manufacturer",
    "manufacurer": "manufacturer",
    "quantitiy": "quantity",
    "quantiy": "quantity",
    "quanity": "quantity",
    "descripton": "description",
    "descriptin": "description",
    "descriptio": "description",
    "desigantor": "designator",
    "designtor": "designator",
    "refernce": "reference",
    "referance": "reference",
    "refernce": "reference",
    "specifcation": "specification",
    "specifiaction": "specification",
    "miscelaneous": "miscellaneous",
    "miscellanous": "miscellaneous",
    "miscelaneous": "miscellaneous",
}


class LexicalSimilarity:
    """Lexical similarity calculator for column name matching."""
    
    def __init__(self):
        """Initialize the lexical similarity calculator."""
        # Build vocabulary from all known column names and aliases
        self.vocabulary: Set[str] = set()
        self._build_vocabulary()
    
    def _build_vocabulary(self):
        """Build vocabulary from known column names."""
        from .schema import COLUMN_MAPPINGS
        
        for standard, variations in COLUMN_MAPPINGS.items():
            self.vocabulary.add(standard)
            for variation in variations:
                tokens = self._tokenize(variation)
                self.vocabulary.update(tokens)
    
    def normalize_text(self, text: str) -> str:
        """Normalize text: lowercase, strip punctuation, split snake/camel, unify tokens.
        
        Args:
            text: Input text to normalize
            
        Returns:
            Normalized text string
        """
        if not text:
            return ""
        
        # Lowercase
        text = text.lower()
        
        # Replace underscores, hyphens, and other separators with spaces
        text = re.sub(r'[_\-\.,;:!?]+', ' ', text)
        
        # Split camelCase and PascalCase
        text = re.sub(r'([a-z])([A-Z])', r'\1 \2', text)
        
        # Split snake_case (already handled by underscore replacement)
        
        # Strip extra whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        
        # Expand abbreviations
        tokens = text.split()
        expanded_tokens = []
        for token in tokens:
            # Remove trailing punctuation
            clean_token = token.rstrip('.,;:!?')
            if clean_token in ABBREVIATION_DICT:
                expanded_tokens.append(ABBREVIATION_DICT[clean_token])
            else:
                expanded_tokens.append(clean_token)
        
        text = ' '.join(expanded_tokens)
        
        return text
    
    def _tokenize(self, text: str) -> Set[str]:
        """Tokenize text into a set of tokens.
        
        Args:
            text: Input text
            
        Returns:
            Set of tokens
        """
        normalized = self.normalize_text(text)
        return set(normalized.split())
    
    def jaro_winkler_similarity(self, s1: str, s2: str, p: float = 0.1) -> float:
        """Calculate Jaro-Winkler similarity between two strings.
        
        Args:
            s1: First string
            s2: Second string
            p: Scaling factor for common prefix (default: 0.1)
            
        Returns:
            Similarity score between 0 and 1
        """
        if not s1 or not s2:
            return 0.0
        
        if s1 == s2:
            return 1.0
        
        # Jaro similarity
        jaro = self._jaro_similarity(s1, s2)
        
        # Find common prefix length (up to 4 characters)
        prefix_len = 0
        min_len = min(len(s1), len(s2), 4)
        for i in range(min_len):
            if s1[i] == s2[i]:
                prefix_len += 1
            else:
                break
        
        # Jaro-Winkler similarity
        jaro_winkler = jaro + (p * prefix_len * (1 - jaro))
        
        return min(jaro_winkler, 1.0)
    
    def _jaro_similarity(self, s1: str, s2: str) -> float:
        """Calculate Jaro similarity between two strings.
        
        Args:
            s1: First string
            s2: Second string
            
        Returns:
            Jaro similarity score between 0 and 1
        """
        if s1 == s2:
            return 1.0
        
        len1, len2 = len(s1), len(s2)
        
        # Match window
        match_window = max(len1, len2) // 2 - 1
        if match_window < 0:
            match_window = 0
        
        # Find matches
        s1_matches = [False] * len1
        s2_matches = [False] * len2
        
        matches = 0
        transpositions = 0
        
        # Find matches
        for i in range(len1):
            start = max(0, i - match_window)
            end = min(i + match_window + 1, len2)
            
            for j in range(start, end):
                if s2_matches[j] or s1[i] != s2[j]:
                    continue
                s1_matches[i] = True
                s2_matches[j] = True
                matches += 1
                break
        
        if matches == 0:
            return 0.0
        
        # Find transpositions
        k = 0
        for i in range(len1):
            if not s1_matches[i]:
                continue
            while not s2_matches[k]:
                k += 1
            if s1[i] != s2[k]:
                transpositions += 1
            k += 1
        
        jaro = (
            matches / len1 +
            matches / len2 +
            (matches - transpositions / 2) / matches
        ) / 3.0
        
        return jaro
    
    def edit_distance(self, s1: str, s2: str) -> int:
        """Calculate Levenshtein edit distance between two strings.
        
        Args:
            s1: First string
            s2: Second string
            
        Returns:
            Edit distance (number of operations needed)
        """
        if not s1:
            return len(s2)
        if not s2:
            return len(s1)
        
        # Dynamic programming approach
        rows = len(s1) + 1
        cols = len(s2) + 1
        
        dist = [[0] * cols for _ in range(rows)]
        
        # Initialize first row and column
        for i in range(1, rows):
            dist[i][0] = i
        for j in range(1, cols):
            dist[0][j] = j
        
        # Fill the distance matrix
        for i in range(1, rows):
            for j in range(1, cols):
                if s1[i-1] == s2[j-1]:
                    cost = 0
                else:
                    cost = 1
                
                dist[i][j] = min(
                    dist[i-1][j] + 1,      # deletion
                    dist[i][j-1] + 1,      # insertion
                    dist[i-1][j-1] + cost  # substitution
                )
        
        return dist[rows-1][cols-1]
    
    def jaccard_similarity(self, s1: str, s2: str) -> float:
        """Calculate Jaccard similarity between two strings using token sets.
        
        Args:
            s1: First string
            s2: Second string
            
        Returns:
            Jaccard similarity score between 0 and 1
        """
        tokens1 = self._tokenize(s1)
        tokens2 = self._tokenize(s2)
        
        if not tokens1 and not tokens2:
            return 1.0
        if not tokens1 or not tokens2:
            return 0.0
        
        intersection = len(tokens1 & tokens2)
        union = len(tokens1 | tokens2)
        
        return intersection / union if union > 0 else 0.0
    
    def cosine_similarity(self, s1: str, s2: str) -> float:
        """Calculate cosine similarity between two strings using token vectors.
        
        Args:
            s1: First string
            s2: Second string
            
        Returns:
            Cosine similarity score between 0 and 1
        """
        tokens1 = self._tokenize(s1)
        tokens2 = self._tokenize(s2)
        
        if not tokens1 and not tokens2:
            return 1.0
        if not tokens1 or not tokens2:
            return 0.0
        
        # Create token frequency vectors
        all_tokens = tokens1 | tokens2
        vec1 = Counter(tokens1)
        vec2 = Counter(tokens2)
        
        # Calculate dot product and magnitudes
        dot_product = sum(vec1.get(token, 0) * vec2.get(token, 0) for token in all_tokens)
        magnitude1 = math.sqrt(sum(count ** 2 for count in vec1.values()))
        magnitude2 = math.sqrt(sum(count ** 2 for count in vec2.values()))
        
        if magnitude1 == 0 or magnitude2 == 0:
            return 0.0
        
        return dot_product / (magnitude1 * magnitude2)
    
    def spell_check(self, word: str, max_distance: int = 2) -> Optional[str]:
        """Simple spell-checker using edit distance (SymSpell-like approach).
        
        Args:
            word: Word to check
            max_distance: Maximum edit distance to consider
            
        Returns:
            Corrected word if found, None otherwise
        """
        if not word:
            return None
        
        word_lower = word.lower()
        
        # Check common misspellings first
        if word_lower in COMMON_MISSPELLINGS:
            return COMMON_MISSPELLINGS[word_lower]
        
        # Check against vocabulary
        best_match = None
        best_distance = max_distance + 1
        
        for vocab_word in self.vocabulary:
            # Only check if lengths are similar
            if abs(len(word_lower) - len(vocab_word)) > max_distance:
                continue
            
            distance = self.edit_distance(word_lower, vocab_word)
            if distance < best_distance and distance <= max_distance:
                best_distance = distance
                best_match = vocab_word
        
        return best_match
    
    def expand_abbreviations(self, text: str) -> str:
        """Expand abbreviations in text using dictionary and statistical expansion.
        
        Args:
            text: Input text with potential abbreviations
            
        Returns:
            Text with abbreviations expanded
        """
        if not text:
            return ""
        
        tokens = text.split()
        expanded_tokens = []
        
        for token in tokens:
            # Remove trailing punctuation
            clean_token = token.rstrip('.,;:!?')
            punct = token[len(clean_token):] if len(token) > len(clean_token) else ""
            
            if clean_token.lower() in ABBREVIATION_DICT:
                expanded = ABBREVIATION_DICT[clean_token.lower()]
                expanded_tokens.append(expanded + punct)
            else:
                expanded_tokens.append(token)
        
        return ' '.join(expanded_tokens)
    
    def calculate_similarity(self, s1: str, s2: str, 
                            use_short_label_heuristic: bool = True) -> float:
        """Calculate combined lexical similarity score between two strings.
        
        Uses multiple features and combines them:
        1. Normalization and token similarity
        2. Edit distance / Jaro-Winkler for short labels
        3. Token similarity (Jaccard / cosine)
        4. Abbreviation expansion
        
        Args:
            s1: First string
            s2: Second string
            use_short_label_heuristic: If True, use Jaro-Winkler for short strings
            
        Returns:
            Combined similarity score between 0 and 1
        """
        if not s1 or not s2:
            return 0.0
        
        if s1 == s2:
            return 1.0
        
        # Normalize both strings
        norm1 = self.normalize_text(s1)
        norm2 = self.normalize_text(s2)
        
        if norm1 == norm2:
            return 1.0
        
        # Determine if we should use short label heuristic
        # Short labels: typically < 15 characters
        is_short = len(norm1) < 15 and len(norm2) < 15
        
        scores = []
        
        # 1. Jaro-Winkler for short labels (good for typos like "mpn" vs "mnp")
        if use_short_label_heuristic and is_short:
            jaro_winkler = self.jaro_winkler_similarity(norm1, norm2)
            scores.append(jaro_winkler)
        
        # 2. Edit distance normalized (for short strings)
        if is_short:
            max_len = max(len(norm1), len(norm2))
            if max_len > 0:
                edit_dist = self.edit_distance(norm1, norm2)
                edit_similarity = 1.0 - (edit_dist / max_len)
                scores.append(edit_similarity)
        
        # 3. Token similarity - Jaccard
        jaccard = self.jaccard_similarity(norm1, norm2)
        scores.append(jaccard)
        
        # 4. Token similarity - Cosine
        cosine = self.cosine_similarity(norm1, norm2)
        scores.append(cosine)
        
        # 5. Try with abbreviation expansion
        expanded1 = self.expand_abbreviations(norm1)
        expanded2 = self.expand_abbreviations(norm2)
        if expanded1 != norm1 or expanded2 != norm2:
            expanded_jaccard = self.jaccard_similarity(expanded1, expanded2)
            scores.append(expanded_jaccard)
        
        # Combine scores (weighted average, with more weight on token similarity)
        if not scores:
            return 0.0
        
        # For short labels, give more weight to character-based metrics
        if is_short and len(scores) > 2:
            # Weight: Jaro-Winkler/Edit: 0.3, Jaccard: 0.3, Cosine: 0.3, Expanded: 0.1
            weights = [0.3, 0.3, 0.2, 0.15, 0.05][:len(scores)]
        else:
            # Weight: Jaccard: 0.4, Cosine: 0.4, Expanded: 0.2
            weights = [0.4, 0.4, 0.2][:len(scores)]
        
        # Normalize weights
        total_weight = sum(weights)
        if total_weight > 0:
            weights = [w / total_weight for w in weights]
        
        combined_score = sum(score * weight for score, weight in zip(scores, weights))
        
        return min(combined_score, 1.0)
    
    def find_best_match(self, query: str, candidates: List[str], 
                       threshold: float = 0.6) -> Optional[Tuple[str, float]]:
        """Find the best matching candidate for a query string.
        
        Args:
            query: Query string to match
            candidates: List of candidate strings
            threshold: Minimum similarity threshold (default: 0.6)
            
        Returns:
            Tuple of (best_match, score) if above threshold, None otherwise
        """
        if not query or not candidates:
            return None
        
        best_match = None
        best_score = 0.0
        
        for candidate in candidates:
            score = self.calculate_similarity(query, candidate)
            if score > best_score:
                best_score = score
                best_match = candidate
        
        if best_score >= threshold:
            return (best_match, best_score)
        
        return None


def should_use_lexical_similarity(column_name: str) -> bool:
    """Determine if lexical similarity should be used for a column.
    
    Part numbers and other non-natural language columns should NOT use
    lexical similarity. Only headers and relevant natural language columns
    (description, quantity, unit, manufacturer, notes) should use it.
    
    Args:
        column_name: Column name to check
        
    Returns:
        True if lexical similarity should be used, False otherwise
    """
    from .schema import COLUMN_MAPPINGS
    
    if not column_name:
        return False
    
    normalized = column_name.lower().strip()
    
    # First, check if this is a NON-lexical column (part numbers, etc.)
    # These should never use lexical similarity
    for standard, variations in COLUMN_MAPPINGS.items():
        if standard in NON_LEXICAL_COLUMNS:
            # Check exact matches and variations
            if normalized in [v.lower() for v in variations]:
                return False
            # Check if the column name contains non-lexical keywords
            if standard in normalized or any(keyword in normalized for keyword in ['part_number', 'mpn', 'ref_des', 'reference_designator']):
                # But be careful - "manufacturer" is lexical, "manufacturer_part_number" is not
                if standard == "manufacturer_part_number" and "manufacturer_part_number" in normalized:
                    return False
                if standard == "part_number" and ("part_number" in normalized or "partnumber" in normalized):
                    return False
    
    # Now check if this column maps to a lexical column
    for standard, variations in COLUMN_MAPPINGS.items():
        if standard in LEXICAL_COLUMNS:
            # Check exact matches
            if normalized in [v.lower() for v in variations]:
                return True
            # Check if it's a variation of a lexical column
            # But exclude if it contains non-lexical keywords
            if any(v.lower() in normalized or normalized in v.lower() for v in variations):
                # Double-check it's not actually a non-lexical column
                if "part_number" in normalized or "mpn" in normalized or "manufacturer_part_number" in normalized:
                    continue
                return True
    
    # Check if normalized form matches a lexical column name directly
    for lexical_col in LEXICAL_COLUMNS:
        if lexical_col == normalized:
            return True
        # Partial match, but be careful about false positives
        if lexical_col in normalized:
            # Make sure it's not part of a non-lexical column name
            if "part_number" not in normalized and "mpn" not in normalized:
                return True
    
    return False

