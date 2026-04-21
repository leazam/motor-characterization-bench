# Unit Tests

This directory contains unit tests for the motor characterization test bench application.

## Running Tests

```bash
# Run all tests
python3 -m pytest tests/ -v

# Run specific test file
python3 -m pytest tests/test_binary_parser.py -v

# Run with short traceback on failures
python3 -m pytest tests/ -v --tb=short
```

## Test Files

| File                        | Tests | Coverage                                                                  |
|-----------------------------|-------|---------------------------------------------------------------------------|
| `test_binary_parser.py`     | 14    | Correct decode, checksum failure, partial packet, resync after corruption |
| `test_csv_parsers.py`       | 19    | Normal read, malformed rows, empty file, missing columns                  |
| `test_yaml_construction.py` | 14    | Parser builds field layout from YAML (binary + CSV)                       |
| `test_synchronization.py`   | 21    | Timestamp alignment, jitter margin handling                               |
| `test_state_machine.py`     | 36    | Phase transitions, abort conditions, safety limits                        |

**Total: 104 tests**

## Test Descriptions

### test_binary_parser.py
Tests for the motor binary protocol parser (`drivers/motor.py`):
- **Correct decode**: Single/multiple valid packets, timestamp monotonicity filtering
- **Checksum failure**: Corrupted checksum detection, discarding invalid packets
- **Partial packet**: Truncated packets at end of file, missing end marker
- **Resync after corruption**: Garbage before/between packets, false start markers
- **Unknown message codes**: Skipping unrecognized response codes

### test_csv_parsers.py
Tests for CSV parsers (motor, sensor, PSU):
- **Normal read**: Valid CSV files with expected columns
- **Malformed rows**: Non-numeric values, empty fields (skip and continue)
- **Empty file**: Proper error on empty CSV or header-only
- **Missing columns**: Error when required columns are absent
- **Edge cases**: Scientific notation, negative values, whitespace handling

### test_yaml_construction.py
Tests that parsers build logic from YAML at runtime (no hardcoded values):
- **Binary parser**: Uses byte order, start marker, response definitions, type definitions from YAML
- **CSV parsers**: Uses column definitions from test_config.yaml
- **State machine**: Uses phase parameters from YAML
- **Configuration loading**: Validates YAML structure

### test_synchronization.py
Tests for multi-rate data synchronization:
- **nearest_prior_index**: Finding samples with timestamp <= target
- **Jitter margin handling**: Conservative approach adds jitter to sensor/PSU timestamps
- **synchronize_data**: Prefixing fields, unified timestamps, lag tracking
- **Edge cases**: No matches, missing timestamps, real jitter scenarios

### test_state_machine.py
Tests for state machine phases and safety:
- **CURRENT_RAMP**: Commanded current ramping, transition on torque/current
- **TORQUE_HOLD**: Constant current, transition after hold duration
- **VOLTAGE_DECREASE**: Commanded voltage decrease, transition at min voltage
- **COMPLETE**: CSV output writing, summary statistics
- **Safety limits**: Torque/current violation detection
- **Abort conditions**: Safety violations, data exhaustion

## Fixtures

Shared fixtures are defined in `conftest.py`:
- `test_config` / `minimal_test_config`: YAML configuration fixtures
- `motor_protocol` / `minimal_motor_protocol`: Binary protocol fixtures
- `build_packet`: Binary packet builder for testing
- `temp_csv_file` / `temp_bin_file`: Temporary file factories
- `sample_*_data`: Sample data for synchronization tests
