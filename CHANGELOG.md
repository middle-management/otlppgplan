# Changelog

## [Unreleased]

### Added
- Native support for PostgreSQL auto_explain log format
  - Automatically parses PostgreSQL log prefix (timestamp, PID, LOG level, duration)
  - Extracts query text from "Query Text" field in auto_explain output
  - Extracts duration from log prefix when available
  - Falls back gracefully to standard EXPLAIN JSON format
- New `AutoExplainLog` struct for parsing auto_explain format
- Automatic format detection between auto_explain and EXPLAIN JSON formats

### Changed
- `Convert()` method now handles both auto_explain and EXPLAIN JSON formats automatically
- Refactored conversion logic into `convertFromRoot()` for code reuse
- Updated examples/compose.yaml to remove transform processor (no longer needed)
- Query text is now automatically populated from auto_explain logs

### Technical Details
- Added regex pattern to parse PostgreSQL log prefix: `duration: X.XXX ms  plan:`
- Added `parseAutoExplainLog()` helper function
- Modified `Convert()` to try auto_explain format first, then fall back to EXPLAIN JSON
- All existing tests continue to pass (backward compatible)

### Documentation
- Added "Supported Input Formats" section to README
- Updated feature list to highlight auto_explain support
- Added example auto_explain log to testdata/examples/autoexplain.json
- Created test_autoexplain.sh to validate auto_explain functionality
