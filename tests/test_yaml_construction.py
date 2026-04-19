"""
Tests for YAML-driven construction.

Tests that parsers correctly build field layouts and configurations
from YAML definitions at runtime (no hardcoded values).
"""

import pytest
import struct

from drivers.motor import load_motor_bin, load_motor_csv
from drivers.sensor import load_sensor_csv
from drivers.psu import load_psu_csv
from automation.state_machine import build_output_field_mapping, get_phase_parameters


# ─────────────────────────────────────────────────────────────
# Binary Parser YAML Construction Tests
# ─────────────────────────────────────────────────────────────

class TestBinaryParserYamlConstruction:
    """Tests that binary parser builds unpacking logic from YAML."""
    
    def test_uses_yaml_byte_order(self, temp_bin_file, minimal_motor_protocol):
        """Parser should use byte order from YAML (little_endian)."""
        # Build packet with little-endian values
        velocity = 150.5
        measured_current = 10.0
        timestamp_ms = 1000
        
        # Build packet manually with little-endian encoding
        start_marker = bytes([0xAA, 0x55])
        header = struct.pack('<BBL', 0x42, 9, timestamp_ms)  # Little-endian
        payload = struct.pack('<B', 0x0E) + struct.pack('<ff', velocity, measured_current)
        
        packet_data = start_marker + header + payload
        checksum = 0
        for b in packet_data:
            checksum ^= b
        packet = packet_data + bytes([checksum]) + bytes([0x55, 0xAA])
        
        bin_path = temp_bin_file("motor.bin", packet)
        
        data, errors = load_motor_bin(bin_path, minimal_motor_protocol)
        
        assert len(data) == 1
        assert abs(data[0]['velocity'] - velocity) < 0.001
    
    def test_uses_yaml_start_marker(self, temp_bin_file, minimal_motor_protocol):
        """Parser should look for start marker defined in YAML."""
        # Create file with wrong start marker - should fail to parse
        wrong_start = bytes([0xBB, 0x66])  # Not 0xAA 0x55
        header = struct.pack('<BBL', 0x42, 9, 1000)
        payload = struct.pack('<Bff', 0x0E, 150.0, 10.0)
        
        packet_data = wrong_start + header + payload
        checksum = 0
        for b in packet_data:
            checksum ^= b
        packet = packet_data + bytes([checksum]) + bytes([0x55, 0xAA])
        
        bin_path = temp_bin_file("motor.bin", packet)
        
        # Should raise error as no valid packets found
        with pytest.raises(ValueError, match="No valid packets"):
            load_motor_bin(bin_path, minimal_motor_protocol)
    
    def test_uses_yaml_response_definitions(self, temp_bin_file, minimal_motor_protocol):
        """Parser should use response field definitions from YAML."""
        # The minimal_motor_protocol defines response 0x0E with velocity and measured_current
        # Both are float32, so 8 bytes payload + 1 byte response code = 9 bytes
        
        packet = build_test_packet(
            response_code=0x0E,
            timestamp_ms=1000,
            payload_fields=[150.5, 10.0],  # velocity, measured_current as float32
            payload_format='<ff'
        )
        bin_path = temp_bin_file("motor.bin", packet)
        
        data, errors = load_motor_bin(bin_path, minimal_motor_protocol)
        
        assert len(data) == 1
        # Field names should come from YAML
        assert 'velocity' in data[0]
        assert 'measured_current' in data[0]
    
    def test_uses_yaml_type_definitions(self, temp_bin_file, minimal_motor_protocol):
        """Parser should use type definitions from YAML for unpacking."""
        # Create a protocol with different types
        modified_protocol = minimal_motor_protocol.copy()
        modified_protocol['responses'] = [
            {
                'code': 0x0E,
                'name': 'telemetry',
                'payload_size': 5,  # 1 response code + 4 bytes (int32 velocity only)
                'fields': [
                    {'name': 'velocity', 'type': 'int32'}  # Changed to int32
                ]
            }
        ]
        
        # Build packet with int32 velocity
        velocity_int = 12345
        packet = build_test_packet(
            response_code=0x0E,
            timestamp_ms=1000,
            payload_fields=[velocity_int],
            payload_format='<i'  # int32
        )
        bin_path = temp_bin_file("motor.bin", packet)
        
        data, errors = load_motor_bin(bin_path, modified_protocol)
        
        assert len(data) == 1
        assert data[0]['velocity'] == float(velocity_int)


class TestBinaryParserHeaderConstruction:
    """Tests that binary parser builds header unpacking from YAML."""
    
    def test_uses_yaml_header_fields(self, temp_bin_file, minimal_motor_protocol):
        """Parser should unpack header fields as defined in YAML."""
        # Standard packet - should extract module_type, payload_size, timestamp_ms
        packet = build_test_packet(
            response_code=0x0E,
            timestamp_ms=5000,
            payload_fields=[100.0, 5.0],
            payload_format='<ff'
        )
        bin_path = temp_bin_file("motor.bin", packet)
        
        data, errors = load_motor_bin(bin_path, minimal_motor_protocol)
        
        assert len(data) == 1
        # timestamp_ms should be converted to timestamp_s
        assert data[0]['timestamp_s'] == 5.0  # 5000ms -> 5.0s


# ─────────────────────────────────────────────────────────────
# CSV Parser YAML Construction Tests
# ─────────────────────────────────────────────────────────────

class TestCsvParserYamlConstruction:
    """Tests that CSV parsers use column definitions from YAML."""
    
    def test_motor_csv_uses_yaml_columns(self, temp_csv_file, minimal_test_config):
        """Motor CSV parser should use columns from test_config.yaml."""
        # Create CSV with exact columns from YAML
        content = "timestamp_s,velocity_rad_s,measured_current_a\n0.001,100.0,1.0\n"
        csv_path = temp_csv_file("motor.csv", content)
        
        data, errors = load_motor_csv(csv_path, minimal_test_config)
        
        assert len(data) == 1
        # Field names should match YAML column names
        assert 'timestamp_s' in data[0]
        assert 'velocity_rad_s' in data[0]
        assert 'measured_current_a' in data[0]
    
    def test_sensor_csv_uses_yaml_columns(self, temp_csv_file, minimal_test_config):
        """Sensor CSV parser should use columns from test_config.yaml."""
        content = "timestamp_s,torque_nm\n0.001,50.0\n"
        csv_path = temp_csv_file("sensor.csv", content)
        
        data, errors = load_sensor_csv(csv_path, minimal_test_config)
        
        assert len(data) == 1
        assert 'timestamp_s' in data[0]
        assert 'torque_nm' in data[0]
    
    def test_psu_csv_uses_yaml_columns(self, temp_csv_file, minimal_test_config):
        """PSU CSV parser should use columns from test_config.yaml."""
        content = "timestamp_s,voltage_v,current_a\n0.0,24.0,0.5\n"
        csv_path = temp_csv_file("psu.csv", content)
        
        data, errors = load_psu_csv(csv_path, minimal_test_config)
        
        assert len(data) == 1
        assert 'timestamp_s' in data[0]
        assert 'voltage_v' in data[0]
        assert 'current_a' in data[0]
    
    def test_modified_yaml_columns_used(self, temp_csv_file):
        """Changing column names in config should change expected columns."""
        # Create config with different column names
        modified_config = {
            'data_sources': {
                'motor': {
                    'formats': {
                        'csv': {
                            'columns': [
                                {'name': 'time', 'type': 'float64'},
                                {'name': 'speed', 'type': 'float64'},
                                {'name': 'amps', 'type': 'float64'}
                            ]
                        }
                    }
                }
            }
        }
        
        content = "time,speed,amps\n0.001,100.0,1.0\n"
        csv_path = temp_csv_file("motor.csv", content)
        
        data, errors = load_motor_csv(csv_path, modified_config)
        
        assert len(data) == 1
        # Should use the modified column names
        assert 'time' in data[0]
        assert 'speed' in data[0]
        assert 'amps' in data[0]


# ─────────────────────────────────────────────────────────────
# State Machine YAML Construction Tests
# ─────────────────────────────────────────────────────────────

class TestStateMachineYamlConstruction:
    """Tests that state machine uses parameters from YAML."""
    
    def test_get_phase_parameters_from_yaml(self, minimal_test_config):
        """get_phase_parameters should return parameters from YAML."""
        params = get_phase_parameters(minimal_test_config, 'CURRENT_RAMP')
        
        assert params is not None
        assert params['max_current_a'] == 34.0
        assert params['ramp_duration_s'] == 10.0
        assert params['target_torque_nm'] == 150.0
    
    def test_get_phase_parameters_unknown_phase(self, minimal_test_config):
        """get_phase_parameters should return None for unknown phase."""
        params = get_phase_parameters(minimal_test_config, 'UNKNOWN_PHASE')
        
        assert params is None
    
    def test_build_output_field_mapping_from_yaml(self, minimal_test_config):
        """build_output_field_mapping should use data_sources from YAML."""
        mapping = build_output_field_mapping(minimal_test_config)
        
        # Should have mappings for output column name variations
        assert mapping is not None
        # velocity_rad_s -> motor_velocity_rad_s (motor prefix added)
        assert 'velocity_rad_s' in mapping
        assert mapping['velocity_rad_s'] == 'motor_velocity_rad_s'
        # torque_nm -> sensor_torque_nm (sensor prefix added)
        assert 'torque_nm' in mapping
        assert mapping['torque_nm'] == 'sensor_torque_nm'


class TestYamlConfigurationLoading:
    """Tests for loading actual YAML configuration files."""
    
    def test_motor_protocol_yaml_loads(self, motor_protocol):
        """motor_protocol.yaml should load with expected structure."""
        assert 'protocol' in motor_protocol
        assert 'framing' in motor_protocol
        assert 'types' in motor_protocol
        assert 'responses' in motor_protocol
        
        # Check protocol metadata
        assert motor_protocol['protocol']['name'] == 'bldc_motor_telemetry'
        assert motor_protocol['protocol']['byte_order'] == 'little_endian'
    
    def test_test_config_yaml_loads(self, test_config):
        """test_config.yaml should load with expected structure."""
        assert 'test' in test_config
        assert 'data_sources' in test_config
        assert 'output' in test_config
        
        # Check phases
        phases = test_config['test']['phases']
        phase_names = [p['name'] for p in phases]
        assert 'SETUP' in phase_names
        assert 'CURRENT_RAMP' in phase_names
        assert 'TORQUE_HOLD' in phase_names
        assert 'VOLTAGE_DECREASE' in phase_names
        assert 'COMPLETE' in phase_names
    
    def test_yaml_data_sources_complete(self, test_config):
        """All data sources should be defined in YAML."""
        sources = test_config['data_sources']
        
        assert 'motor' in sources
        assert 'sensor' in sources
        assert 'power_supply' in sources
        
        # Each should have CSV format defined
        assert 'csv' in sources['motor']['formats']
        assert 'csv' in sources['sensor']['formats']
        assert 'csv' in sources['power_supply']['formats']


# ─────────────────────────────────────────────────────────────
# Helper Functions
# ─────────────────────────────────────────────────────────────

def build_test_packet(
    response_code: int,
    timestamp_ms: int,
    payload_fields: list,
    payload_format: str
) -> bytes:
    """Build a test binary packet with given parameters."""
    start_marker = bytes([0xAA, 0x55])
    end_marker = bytes([0x55, 0xAA])
    
    # Build payload
    payload = struct.pack('<B', response_code)
    payload += struct.pack(payload_format, *payload_fields)
    
    payload_size = len(payload)
    
    # Header: module_type, payload_size, timestamp_ms
    header = struct.pack('<BBL', 0x42, payload_size, timestamp_ms)
    
    # Compute checksum
    packet_data = start_marker + header + payload
    checksum = 0
    for b in packet_data:
        checksum ^= b
    
    return packet_data + bytes([checksum]) + end_marker
