"""Lexical similarity utilities for column header matching."""

import math
import re
from typing import List


class LexicalSimilarity:
    """Lightweight lexical similarity helpers."""

    def normalize_text(self, text: str) -> str:
        text = text.lower().strip()
        text = re.sub(r'[_\-\.\s]+', ' ', text)
        text = re.sub(r'[^\w\s]', '', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def _tokenize(self, text: str) -> List[str]:
        normalized = self.normalize_text(text)
        if not normalized:
            return []
        return normalized.split()

    def jaro_winkler_similarity(self, s1: str, s2: str, p: float = 0.1) -> float:
        s1 = self.normalize_text(s1)
        s2 = self.normalize_text(s2)
        if s1 == s2:
            return 1.0
        if not s1 or not s2:
            return 0.0

        match_distance = max(len(s1), len(s2)) // 2 - 1
        s1_matches = [False] * len(s1)
        s2_matches = [False] * len(s2)

        matches = 0
        for i, ch in enumerate(s1):
            start = max(0, i - match_distance)
            end = min(i + match_distance + 1, len(s2))
            for j in range(start, end):
                if s2_matches[j]:
                    continue
                if ch == s2[j]:
                    s1_matches[i] = True
                    s2_matches[j] = True
                    matches += 1
                    break

        if matches == 0:
            return 0.0

        transpositions = 0
        k = 0
        for i, matched in enumerate(s1_matches):
            if not matched:
                continue
            while not s2_matches[k]:
                k += 1
            if s1[i] != s2[k]:
                transpositions += 1
            k += 1
        transpositions /= 2

        m = matches
        jaro = (m / len(s1) + m / len(s2) + (m - transpositions) / m) / 3

        # Winkler boost for common prefix
        prefix = 0
        for ch1, ch2 in zip(s1, s2):
            if ch1 == ch2:
                prefix += 1
            else:
                break
            if prefix == 4:
                break

        return jaro + prefix * p * (1 - jaro)

    def jaccard_similarity(self, s1: str, s2: str) -> float:
        t1 = set(self._tokenize(s1))
        t2 = set(self._tokenize(s2))
        if not t1 and not t2:
            return 1.0
        if not t1 or not t2:
            return 0.0
        return len(t1 & t2) / len(t1 | t2)

    def cosine_similarity(self, s1: str, s2: str) -> float:
        tokens1 = self._tokenize(s1)
        tokens2 = self._tokenize(s2)
        if not tokens1 and not tokens2:
            return 1.0
        if not tokens1 or not tokens2:
            return 0.0

        freq1 = {}
        freq2 = {}
        for t in tokens1:
            freq1[t] = freq1.get(t, 0) + 1
        for t in tokens2:
            freq2[t] = freq2.get(t, 0) + 1

        all_tokens = set(freq1.keys()) | set(freq2.keys())
        dot = sum(freq1.get(t, 0) * freq2.get(t, 0) for t in all_tokens)
        norm1 = math.sqrt(sum(v * v for v in freq1.values()))
        norm2 = math.sqrt(sum(v * v for v in freq2.values()))

        if norm1 == 0 or norm2 == 0:
            return 0.0
        return dot / (norm1 * norm2)

    def spell_check(self, text: str) -> str:
        """Very small correction pass for common BOM header typos."""
        normalized = self.normalize_text(text)
        corrections = {
            "quantitiy": "quantity",
            "manufaturer": "manufacturer",
            "descripton": "description",
            "maufacturer": "manufacturer",
            "manufacterer": "manufacturer",
            "manf": "manufacturer",
        }
        return corrections.get(normalized, normalized)

    def calculate_similarity(self, s1: str, s2: str) -> float:
        """Combined similarity for short labels and token overlap."""
        jw = self.jaro_winkler_similarity(s1, s2)
        jaccard = self.jaccard_similarity(s1, s2)
        cosine = self.cosine_similarity(s1, s2)
        return 0.5 * jw + 0.25 * jaccard + 0.25 * cosine


def should_use_lexical_similarity(column_name: str) -> bool:
    """Return True if lexical similarity is appropriate for the column."""
    name = column_name.lower()
    # Use lexical similarity primarily for header matching, not content-like fields.
    return any(
        key in name
        for key in [
            "description",
            "part",
            "manufacturer",
            "supplier",
            "vendor",
            "distributor",
            "qty",
            "quantity",
            "unit",
            "uom",
            "reference",
            "refdes",
        ]
    )
