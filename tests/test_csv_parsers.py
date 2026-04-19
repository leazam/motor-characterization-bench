"""
Tests for CSV parsers (motor, sensor, PSU).

Tests cover:
- Normal read with valid data
- Malformed rows (non-numeric values, missing fields)
- Empty file handling
- Missing columns in header
"""

import pytest
from pathlib import Path

from drivers.motor import load_motor_csv
from drivers.sensor import load_sensor_csv
from drivers.psu import load_psu_csv


# ─────────────────────────────────────────────────────────────
# Motor CSV Parser Tests
# ─────────────────────────────────────────────────────────────

class TestMotorCsvNormalRead:
    """Tests for normal motor CSV parsing."""
    
    def test_valid_motor_csv(self, temp_csv_file, minimal_test_config):
        """Valid motor CSV should parse correctly."""
        content = "timestamp_s,velocity_rad_s,measured_current_a\n0.001,100.5,1.2\n0.002,101.0,1.3\n0.003,101.5,1.4\n"
        csv_path = temp_csv_file("motor.csv", content)
        
        data, errors = load_motor_csv(csv_path, minimal_test_config)
        
        assert len(data) == 3
        assert len(errors) == 0
        assert data[0]['timestamp_s'] == 0.001
        assert data[0]['velocity_rad_s'] == 100.5
        assert data[0]['measured_current_a'] == 1.2
        assert data[2]['timestamp_s'] == 0.003
    
    def test_motor_csv_monotonicity_filter(self, temp_csv_file, minimal_test_config):
        """Non-monotonic timestamps should be filtered out."""
        content = "timestamp_s,velocity_rad_s,measured_current_a\n0.001,100.0,1.0\n0.003,102.0,1.2\n0.002,101.0,1.1\n0.004,103.0,1.3\n"
        csv_path = temp_csv_file("motor.csv", content)
        
        data, errors = load_motor_csv(csv_path, minimal_test_config)
        
        # Row with timestamp 0.002 should be skipped (comes after 0.003)
        assert len(data) == 3
        assert data[0]['timestamp_s'] == 0.001
        assert data[1]['timestamp_s'] == 0.003
        assert data[2]['timestamp_s'] == 0.004
        assert any('non-monotonic' in e.lower() or 'Non-monotonic' in e for e in errors)


class TestMotorCsvMalformedRows:
    """Tests for malformed row handling in motor CSV."""
    
    def test_non_numeric_value_skipped(self, temp_csv_file, minimal_test_config):
        """Rows with non-numeric values should be skipped."""
        content = "timestamp_s,velocity_rad_s,measured_current_a\n0.001,100.0,1.0\n0.002,abc,1.1\n0.003,102.0,1.2\n"
        csv_path = temp_csv_file("motor.csv", content)
        
        data, errors = load_motor_csv(csv_path, minimal_test_config)
        
        assert len(data) == 2
        assert len(errors) == 1
        assert data[0]['timestamp_s'] == 0.001
        assert data[1]['timestamp_s'] == 0.003
        assert 'abc' in errors[0] or 'non-numeric' in errors[0].lower()
    
    def test_empty_field_skipped(self, temp_csv_file, minimal_test_config):
        """Rows with empty fields should be skipped."""
        content = "timestamp_s,velocity_rad_s,measured_current_a\n0.001,100.0,1.0\n0.002,,1.1\n0.003,102.0,1.2\n"
        csv_path = temp_csv_file("motor.csv", content)
        
        data, errors = load_motor_csv(csv_path, minimal_test_config)
        
        assert len(data) == 2
        assert len(errors) == 1


class TestMotorCsvEmptyFile:
    """Tests for empty file handling."""
    
    def test_empty_file_raises_error(self, temp_csv_file, minimal_test_config):
        """Empty CSV file should raise ValueError."""
        csv_path = temp_csv_file("motor.csv", "")
        
        with pytest.raises(ValueError, match="Empty CSV file"):
            load_motor_csv(csv_path, minimal_test_config)
    
    def test_header_only_raises_error(self, temp_csv_file, minimal_test_config):
        """CSV with only header (no data rows) should raise ValueError."""
        content = "timestamp_s,velocity_rad_s,measured_current_a\n"
        csv_path = temp_csv_file("motor.csv", content)
        
        with pytest.raises(ValueError, match="No valid data rows"):
            load_motor_csv(csv_path, minimal_test_config)


class TestMotorCsvMissingColumns:
    """Tests for missing column handling."""
    
    def test_missing_required_column_raises_error(self, temp_csv_file, minimal_test_config):
        """Missing required column in header should raise ValueError."""
        # Missing velocity_rad_s column
        content = "timestamp_s,measured_current_a\n0.001,1.0\n"
        csv_path = temp_csv_file("motor.csv", content)
        
        with pytest.raises(ValueError, match="Missing required columns"):
            load_motor_csv(csv_path, minimal_test_config)
    
    def test_extra_columns_ignored(self, temp_csv_file, minimal_test_config):
        """Extra columns in CSV should be ignored (no error)."""
        content = "timestamp_s,velocity_rad_s,measured_current_a,extra_column\n0.001,100.0,1.0,999\n"
        csv_path = temp_csv_file("motor.csv", content)
        
        data, errors = load_motor_csv(csv_path, minimal_test_config)
        
        assert len(data) == 1
        assert 'extra_column' not in data[0]


# ─────────────────────────────────────────────────────────────
# Sensor CSV Parser Tests
# ─────────────────────────────────────────────────────────────

class TestSensorCsvNormalRead:
    """Tests for normal sensor CSV parsing."""
    
    def test_valid_sensor_csv(self, temp_csv_file, minimal_test_config):
        """Valid sensor CSV should parse correctly."""
        content = "timestamp_s,torque_nm\n0.0002,10.5\n0.0004,11.0\n0.0006,11.5\n"
        csv_path = temp_csv_file("sensor.csv", content)
        
        data, errors = load_sensor_csv(csv_path, minimal_test_config)
        
        assert len(data) == 3
        assert len(errors) == 0
        assert data[0]['timestamp_s'] == 0.0002
        assert data[0]['torque_nm'] == 10.5


class TestSensorCsvMalformedRows:
    """Tests for malformed row handling in sensor CSV."""
    
    def test_non_numeric_torque_skipped(self, temp_csv_file, minimal_test_config):
        """Rows with non-numeric torque should be skipped."""
        content = "timestamp_s,torque_nm\n0.0002,10.5\n0.0004,bad\n0.0006,11.5\n"
        csv_path = temp_csv_file("sensor.csv", content)
        
        data, errors = load_sensor_csv(csv_path, minimal_test_config)
        
        assert len(data) == 2
        assert len(errors) == 1


class TestSensorCsvEmptyFile:
    """Tests for empty sensor file handling."""
    
    def test_empty_sensor_file_raises_error(self, temp_csv_file, minimal_test_config):
        """Empty sensor CSV should raise ValueError."""
        csv_path = temp_csv_file("sensor.csv", "")
        
        with pytest.raises(ValueError, match="Empty CSV file"):
            load_sensor_csv(csv_path, minimal_test_config)


class TestSensorCsvMissingColumns:
    """Tests for missing column handling in sensor CSV."""
    
    def test_missing_torque_column_raises_error(self, temp_csv_file, minimal_test_config):
        """Missing torque_nm column should raise ValueError."""
        content = "timestamp_s,other_column\n0.001,123\n"
        csv_path = temp_csv_file("sensor.csv", content)
        
        with pytest.raises(ValueError, match="Missing required columns"):
            load_sensor_csv(csv_path, minimal_test_config)


# ─────────────────────────────────────────────────────────────
# PSU CSV Parser Tests
# ─────────────────────────────────────────────────────────────

class TestPsuCsvNormalRead:
    """Tests for normal PSU CSV parsing."""
    
    def test_valid_psu_csv(self, temp_csv_file, minimal_test_config):
        """Valid PSU CSV should parse correctly."""
        content = "timestamp_s,voltage_v,current_a\n0.0,24.0,0.5\n0.1,24.0,0.6\n0.2,24.0,0.7\n"
        csv_path = temp_csv_file("psu.csv", content)
        
        data, errors = load_psu_csv(csv_path, minimal_test_config)
        
        assert len(data) == 3
        assert len(errors) == 0
        assert data[0]['timestamp_s'] == 0.0
        assert data[0]['voltage_v'] == 24.0
        assert data[0]['current_a'] == 0.5


class TestPsuCsvMalformedRows:
    """Tests for malformed row handling in PSU CSV."""
    
    def test_non_numeric_voltage_skipped(self, temp_csv_file, minimal_test_config):
        """Rows with non-numeric voltage should be skipped."""
        content = "timestamp_s,voltage_v,current_a\n0.0,24.0,0.5\n0.1,N/A,0.6\n0.2,24.0,0.7\n"
        csv_path = temp_csv_file("psu.csv", content)
        
        data, errors = load_psu_csv(csv_path, minimal_test_config)
        
        assert len(data) == 2
        assert len(errors) == 1


class TestPsuCsvEmptyFile:
    """Tests for empty PSU file handling."""
    
    def test_empty_psu_file_raises_error(self, temp_csv_file, minimal_test_config):
        """Empty PSU CSV should raise ValueError."""
        csv_path = temp_csv_file("psu.csv", "")
        
        with pytest.raises(ValueError, match="Empty CSV file"):
            load_psu_csv(csv_path, minimal_test_config)


class TestPsuCsvMissingColumns:
    """Tests for missing column handling in PSU CSV."""
    
    def test_missing_voltage_column_raises_error(self, temp_csv_file, minimal_test_config):
        """Missing voltage_v column should raise ValueError."""
        content = "timestamp_s,current_a\n0.0,0.5\n"
        csv_path = temp_csv_file("psu.csv", content)
        
        with pytest.raises(ValueError, match="Missing required columns"):
            load_psu_csv(csv_path, minimal_test_config)


# ─────────────────────────────────────────────────────────────
# Edge Cases Across All Parsers
# ─────────────────────────────────────────────────────────────

class TestCsvEdgeCases:
    """Edge case tests applicable to all CSV parsers."""
    
    def test_scientific_notation_parsed(self, temp_csv_file, minimal_test_config):
        """Scientific notation should be parsed correctly."""
        content = "timestamp_s,velocity_rad_s,measured_current_a\n1e-3,1.005e2,1.2e0\n"
        csv_path = temp_csv_file("motor.csv", content)
        
        data, errors = load_motor_csv(csv_path, minimal_test_config)
        
        assert len(data) == 1
        assert abs(data[0]['timestamp_s'] - 0.001) < 1e-9
        assert abs(data[0]['velocity_rad_s'] - 100.5) < 0.001
    
    def test_negative_values_parsed(self, temp_csv_file, minimal_test_config):
        """Negative values should be parsed correctly."""
        content = "timestamp_s,torque_nm\n0.001,-50.5\n0.002,-60.0\n"
        csv_path = temp_csv_file("sensor.csv", content)
        
        data, errors = load_sensor_csv(csv_path, minimal_test_config)
        
        assert len(data) == 2
        assert data[0]['torque_nm'] == -50.5
    
    def test_whitespace_in_values_handled(self, temp_csv_file, minimal_test_config):
        """Leading/trailing whitespace in values should be handled."""
        # Note: csv.DictReader typically preserves whitespace, but float() strips it
        content = "timestamp_s,velocity_rad_s,measured_current_a\n 0.001 , 100.5 , 1.2 \n"
        csv_path = temp_csv_file("motor.csv", content)
        
        data, errors = load_motor_csv(csv_path, minimal_test_config)
        
        # Should parse successfully (float() handles whitespace)
        assert len(data) == 1
        assert data[0]['timestamp_s'] == 0.001
