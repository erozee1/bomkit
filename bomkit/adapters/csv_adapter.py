import csv
import chardet
import re
from pathlib import Path
from typing import List, Dict, Any, Iterable, Tuple

from bomkit.schema import COLUMN_MAPPINGS, STANDARD_HEADERS


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
        """Detect CSV delimiter by analyzing the first few lines."""
        path = Path(file_path)
        suffix = path.suffix.lower()

        # TSV files use tab delimiter
        if suffix == '.tsv':
            return '\t'

        # For CSV, try to detect delimiter
        try:
            with open(file_path, 'r', encoding=encoding, newline='') as f:
                sample_lines = []
                for _ in range(10):
                    line = f.readline()
                    if not line:
                        break
                    if line.strip():
                        sample_lines.append(line)

                if not sample_lines:
                    return ','

                candidates = [',', ';', '\t']
                avg_counts = {}
                for delimiter in candidates:
                    counts = [line.count(delimiter) for line in sample_lines]
                    avg_counts[delimiter] = sum(counts) / len(counts)

                # Choose delimiter with highest average count
                best = max(avg_counts.items(), key=lambda item: item[1])[0]

                # If all averages are zero, fall back to comma
                if avg_counts[best] == 0:
                    return ','

                return best
        except Exception:
            # Default to comma if detection fails
            return ','

    def _normalize_header_text(self, text: str) -> str:
        """Normalize header text for scoring."""
        text = text.lower().strip()
        text = re.sub(r'[\s_\-]+', ' ', text)
        return text

    def _build_header_aliases(self) -> set:
        aliases = set(self._normalize_header_text(header) for header in STANDARD_HEADERS)
        for variations in COLUMN_MAPPINGS.values():
            for variation in variations:
                aliases.add(self._normalize_header_text(variation))
        return aliases

    def _score_header_row(self, row: List[str], header_aliases: set) -> Tuple[float, int]:
        """Score a potential header row.

        Returns:
            (score, header_hits)
        """
        if not row:
            return 0.0, 0

        normalized_cells = [self._normalize_header_text(cell) for cell in row]
        non_empty = [cell for cell in normalized_cells if cell]

        if len(non_empty) < 2:
            return 0.0, 0

        header_hits = 0
        partial_hits = 0
        alpha_cells = 0
        numeric_cells = 0

        for cell in non_empty:
            if cell in header_aliases:
                header_hits += 1
            else:
                for alias in header_aliases:
                    if alias and (alias in cell or cell in alias):
                        partial_hits += 1
                        break

            if re.search(r'[a-zA-Z]', cell):
                alpha_cells += 1
            if re.fullmatch(r'[\d\.\-]+', cell):
                numeric_cells += 1

        # Weighted score: prioritize known header aliases and text-heavy rows
        score = 0.0
        score += header_hits * 2.0
        score += partial_hits * 0.5
        score += alpha_cells * 0.3
        score -= numeric_cells * 0.5
        score += (len(non_empty) / max(len(row), 1)) * 0.2

        return score, header_hits

    def _select_header_row(self, rows: List[List[str]]) -> int:
        """Select the best header row index from the first N rows."""
        if not rows:
            return -1

        header_aliases = self._build_header_aliases()
        best_idx = -1
        best_score = 0.0
        best_hits = 0

        for idx, row in enumerate(rows[:50]):
            score, hits = self._score_header_row(row, header_aliases)
            if score > best_score:
                best_score = score
                best_hits = hits
                best_idx = idx

        # Require a minimum quality for a header row
        if best_idx >= 0 and (best_score >= 2.0 or best_hits >= 2):
            return best_idx

        return -1

    def _dedupe_headers(self, headers: List[str]) -> List[str]:
        """Ensure headers are unique by suffixing duplicates."""
        seen = {}
        result = []
        for header in headers:
            key = header.strip() if header is not None else ""
            if key == "":
                key = "column"
            if key not in seen:
                seen[key] = 1
                result.append(key)
            else:
                seen[key] += 1
                result.append(f"{key}_{seen[key]}")
        return result

    def _rows_to_dicts(self, headers: List[str], rows: Iterable[List[str]], delimiter: str) -> List[Dict[str, Any]]:
        """Convert rows to dictionaries, padding or merging as needed."""
        data = []
        header_len = len(headers)
        for row in rows:
            if not any(cell.strip() for cell in row if cell is not None):
                continue
            if len(row) < header_len:
                row = row + [''] * (header_len - len(row))
            elif len(row) > header_len:
                extras = row[header_len - 1:]
                row = row[:header_len - 1] + [delimiter.join(extras)]

            cleaned_row = {
                headers[i]: str(row[i]) if row[i] is not None else ''
                for i in range(header_len)
            }
            data.append(cleaned_row)
        return data

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
                    sample = f.read(2048)
                    f.seek(0)
                    sniffer = csv.Sniffer()
                    dialect = sniffer.sniff(sample, delimiters=',;\t')
                    delimiter = dialect.delimiter
                except (csv.Error, Exception):
                    # Fall back to detected delimiter
                    pass

                reader = csv.reader(f, delimiter=delimiter)
                all_rows = [row for row in reader]

                if not all_rows:
                    return []

                header_idx = self._select_header_row(all_rows)

                if header_idx >= 0:
                    headers = self._dedupe_headers(all_rows[header_idx])
                    data_rows = all_rows[header_idx + 1:]
                else:
                    max_len = max(len(r) for r in all_rows)
                    headers = self._dedupe_headers([f"column_{i+1}" for i in range(max_len)])
                    data_rows = all_rows

                rows = self._rows_to_dicts(headers, data_rows, delimiter)

        except UnicodeDecodeError as e:
            # Try with different encoding as fallback
            fallback_encodings = ['latin-1', 'cp1252', 'iso-8859-1']
            for fallback_encoding in fallback_encodings:
                try:
                    with open(file_path, 'r', encoding=fallback_encoding, newline='') as f:
                        reader = csv.reader(f, delimiter=delimiter)
                        all_rows = [row for row in reader]
                        if not all_rows:
                            return []
                        header_idx = self._select_header_row(all_rows)
                        if header_idx >= 0:
                            headers = self._dedupe_headers(all_rows[header_idx])
                            data_rows = all_rows[header_idx + 1:]
                        else:
                            max_len = max(len(r) for r in all_rows)
                            headers = self._dedupe_headers([f"column_{i+1}" for i in range(max_len)])
                            data_rows = all_rows
                        rows = self._rows_to_dicts(headers, data_rows, delimiter)
                        break
                except (UnicodeDecodeError, Exception):
                    continue
            else:
                raise ValueError(f"Could not decode file {file_path}: {e}")

        except csv.Error as e:
            raise ValueError(f"Error parsing CSV file {file_path}: {e}")

        return rows
