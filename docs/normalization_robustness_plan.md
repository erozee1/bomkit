# BOMKit Normalization & Ingestion Robustness Plan

## Problem Statement
Our current BOM ingestion assumes a clean header row, consistent delimiter, and conventional column names. Real BOM files often include preamble metadata rows, inconsistent dialects, and ambiguous headers. This leads to incorrect column identification and brittle normalization.

## Goals
- Reliably parse BOM files with preamble rows, mixed delimiters, and imperfect headers.
- Improve column-to-schema mapping with data-driven inference, not only header names.
- Provide traceable mapping decisions and confidence so failures are diagnosable.
- Preserve unmapped data without losing information.

## Non-Goals (for this phase)
- Perfect semantic matching for all enterprise schemas.
- Full ETL pipeline or UI workflows.
- Automated external metadata discovery (e.g., CSVW sidecar files).

## Observations From Current Test Files
- `Desktop FDM 3D Printer.csv` has multiple metadata rows before the real header.
- `BOM-3.csv` uses semicolon delimiter and embeds commas within values.
- `BOM-4.csv` uses short/abbreviated headers like “Cmp name”.

## Proposed End-to-End Flow
1. **Dialect Detection**: Identify delimiter and encoding using multi-line sampling; tolerate mixed/dirty CSV variants.
2. **Header Detection**: Score the first N rows for header-likeness using known aliases + text heuristics. Pick the highest-confidence row.
3. **Schema Mapping**:
   - Name-based mapping from known aliases.
   - Data profiling (type distribution, regex hits, units) to disambiguate.
   - Conflict resolution when multiple columns map to the same field.
4. **Normalization**: Convert to standard schema and normalize refdes ranges.
5. **Validation & Telemetry**:
   - Report mapped/unmapped columns.
   - Track mapping confidence and failure reasons.

## Best Practices Incorporated
- **Explicit schema definitions**: Align to a fixed canonical schema with clear field expectations (types, constraints).
- **Separation of dialect parsing vs. semantic mapping**: Keep CSV parsing independent from schema inference.
- **Data profiling + validation**: Generate column statistics to detect type anomalies and measure mapping confidence.
- **Lossless ingestion**: Preserve unmapped data in `notes` instead of dropping it.

## Cutting-Edge Approaches (Future Extensions)
- **Embedding-based schema matching**: Use column/value embeddings to match semantically similar fields even with unknown headers.
- **Retrieval-augmented matching**: Combine learned retrieval with LLM-based reranking to improve matching precision on large, heterogeneous schemas.
- **Hybrid matchers**: Use lightweight models for candidate selection and LLMs for final ranking.

## Implementation Plan
### Phase 1 (Now)
- Improve CSV adapter with robust delimiter detection and header row selection.
- Add column profiling-driven mapping and conflict resolution.
- Add tests for preamble headers and short/abbreviated column names.

### Phase 2 (Next)
- Add confidence scores and mapping diagnostics to outputs.
- Add optional schema validation or quality checks (type/format expectations).
- Add config to tune thresholds per customer.

### Phase 3 (R&D)
- Evaluate embedding-based schema matching for unknown BOMs.
- Add human-in-the-loop corrections to improve mapping over time.

## Metrics / Acceptance Criteria
- ≥95% correct field mapping across our test corpus (including non-standard headers).
- 0 parsing failures on preamble-heavy CSVs.
- Unmapped columns preserved and visible in normalization output.

## Risks & Mitigations
- **False positives in header detection**: Mitigate with scoring thresholds and fail-safe to synthetic headers.
- **Overfitting to small corpus**: Mitigate with configurable thresholds and diagnostics.
- **Ambiguous columns**: Mitigate with data profiling + conflict resolution.

## Rollout
- Ship Phase 1 improvements behind existing defaults (no API break).
- Add regression tests for each new failure case.
