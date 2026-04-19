"""
Shared pytest fixtures for motor characterization tests.
"""

import pytest
import yaml
import tempfile
import struct
from pathlib import Path
from typing import Dict, Any, List


# ─────────────────────────────────────────────────────────────
# YAML Configuration Fixtures
# ─────────────────────────────────────────────────────────────

@pytest.fixture
def test_config() -> Dict[str, Any]:
    """Load test_config.yaml from the config directory."""
    config_path = Path(__file__).parent.parent / "config" / "test_config.yaml"
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


@pytest.fixture
def motor_protocol() -> Dict[str, Any]:
    """Load motor_protocol.yaml from the config directory."""
    config_path = Path(__file__).parent.parent / "config" / "motor_protocol.yaml"
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


# ─────────────────────────────────────────────────────────────
# Minimal YAML Config Fixtures (for isolated tests)
# ─────────────────────────────────────────────────────────────

@pytest.fixture
def minimal_test_config() -> Dict[str, Any]:
    """Minimal test_config for unit tests that don't need full config."""
    return {
        'test': {
            'name': 'test',
            'phases': [
                {'name': 'SETUP'},
                {
                    'name': 'CURRENT_RAMP',
                    'parameters': {
                        'max_current_a': 34.0,
                        'ramp_duration_s': 10.0,
                        'target_torque_nm': 150.0
                    }
                },
                {
                    'name': 'TORQUE_HOLD',
                    'parameters': {'hold_duration_s': 10.0}
                },
                {
                    'name': 'VOLTAGE_DECREASE',
                    'parameters': {
                        'voltage_decrease_rate_v_per_s': 1.0,
                        'min_voltage_v': 0.0
                    }
                },
                {'name': 'COMPLETE'}
            ],
            'safety': {
                'max_torque_nm': 200.0,
                'max_current_a': 34.0
            }
        },
        'power_supply': {
            'initial_voltage_v': 24.0
        },
        'data_sources': {
            'motor': {
                'formats': {
                    'csv': {
                        'columns': [
                            {'name': 'timestamp_s', 'type': 'float64'},
                            {'name': 'velocity_rad_s', 'type': 'float64'},
                            {'name': 'measured_current_a', 'type': 'float64'}
                        ]
                    }
                }
            },
            'sensor': {
                'formats': {
                    'csv': {
                        'columns': [
                            {'name': 'timestamp_s', 'type': 'float64'},
                            {'name': 'torque_nm', 'type': 'float64'}
                        ]
                    }
                }
            },
            'power_supply': {
                'formats': {
                    'csv': {
                        'columns': [
                            {'name': 'timestamp_s', 'type': 'float64'},
                            {'name': 'voltage_v', 'type': 'float64'},
                            {'name': 'current_a', 'type': 'float64'}
                        ]
                    }
                }
            }
        },
        'output': {
            'columns': [
                'timestamp_s', 'velocity_rad_s', 'motor_current_a',
                'torque_nm', 'psu_voltage_v', 'psu_current_a',
                'commanded_current_a', 'commanded_voltage_v', 'test_phase'
            ]
        }
    }


@pytest.fixture
def minimal_motor_protocol() -> Dict[str, Any]:
    """Minimal motor_protocol for unit tests."""
    return {
        'protocol': {
            'name': 'bldc_motor_telemetry',
            'version': '1.0',
            'byte_order': 'little_endian'
        },
        'framing': {
            'start_marker': {'bytes': [0xAA, 0x55]},
            'end_marker': {'bytes': [0x55, 0xAA]},
            'header': {
                'fields': [
                    {'name': 'module_type', 'type': 'uint8'},
                    {'name': 'payload_size', 'type': 'uint8'},
                    {'name': 'timestamp_ms', 'type': 'uint32'}
                ]
            },
            'checksum': {'type': 'xor', 'size': 1}
        },
        'types': {
            'uint8': {'size': 1, 'signed': False, 'format': 'B'},
            'uint16': {'size': 2, 'signed': False, 'format': '<H'},
            'uint32': {'size': 4, 'signed': False, 'format': '<I'},
            'int32': {'size': 4, 'signed': True, 'format': '<i'},
            'float32': {'size': 4, 'signed': True, 'format': '<f'}
        },
        'responses': [
            {
                'code': 0x0E,
                'name': 'telemetry',
                'payload_size': 9,
                'fields': [
                    {'name': 'velocity', 'type': 'float32', 'unit': 'rad/s'},
                    {'name': 'measured_current', 'type': 'float32', 'unit': 'A'}
                ]
            }
        ],
        'commands': [
            {
                'code': 0x06,
                'name': 'set_current',
                'fields': [
                    {'name': 'motor_id', 'type': 'uint8'},
                    {'name': 'current_index', 'type': 'uint8'},
                    {'name': 'current', 'type': 'float32', 'unit': 'A'}
                ]
            }
        ]
    }


# ─────────────────────────────────────────────────────────────
# Binary Packet Builder
# ─────────────────────────────────────────────────────────────

def build_motor_packet(
    timestamp_ms: int,
    velocity: float,
    measured_current: float,
    response_code: int = 0x0E,
    module_type: int = 0x42,
    corrupt_checksum: bool = False,
    omit_end_marker: bool = False
) -> bytes:
    """
    Build a valid binary motor packet for testing.
    
    Packet structure: [start][header][payload][checksum][end]
    - Start marker: 0xAA 0x55
    - Header: module_type (1), payload_size (1), timestamp_ms (4)
    - Payload: response_code (1), velocity (4), measured_current (4)
    - Checksum: XOR of all preceding bytes
    - End marker: 0x55 0xAA
    """
    start_marker = bytes([0xAA, 0x55])
    end_marker = bytes([0x55, 0xAA])
    
    # Payload: response_code + velocity + measured_current
    payload = struct.pack('<B', response_code)  # response code
    payload += struct.pack('<f', velocity)       # velocity (float32)
    payload += struct.pack('<f', measured_current)  # measured_current (float32)
    
    payload_size = len(payload)
    
    # Header: module_type, payload_size, timestamp_ms
    header = struct.pack('<BBL', module_type, payload_size, timestamp_ms)
    
    # Compute XOR checksum over start_marker + header + payload
    packet_data = start_marker + header + payload
    checksum = 0
    for byte in packet_data:
        checksum ^= byte
    
    if corrupt_checksum:
        checksum ^= 0xFF  # Flip all bits to corrupt
    
    result = packet_data + bytes([checksum])
    
    if not omit_end_marker:
        result += end_marker
    
    return result


@pytest.fixture
def build_packet():
    """Fixture providing the packet builder function."""
    return build_motor_packet


# ─────────────────────────────────────────────────────────────
# CSV File Fixtures
# ─────────────────────────────────────────────────────────────

@pytest.fixture
def temp_csv_file(tmp_path):
    """
    Factory fixture to create temporary CSV files.
    
    Usage:
        csv_path = temp_csv_file("motor.csv", "timestamp_s,velocity_rad_s\\n0.001,100.5\\n")
    """
    def _create_csv(filename: str, content: str) -> Path:
        filepath = tmp_path / filename
        filepath.write_text(content, encoding='utf-8')
        return filepath
    return _create_csv


@pytest.fixture
def temp_bin_file(tmp_path):
    """
    Factory fixture to create temporary binary files.
    
    Usage:
        bin_path = temp_bin_file("motor.bin", bytes([0xAA, 0x55, ...]))
    """
    def _create_bin(filename: str, content: bytes) -> Path:
        filepath = tmp_path / filename
        filepath.write_bytes(content)
        return filepath
    return _create_bin


# ─────────────────────────────────────────────────────────────
# Sample Data Fixtures
# ─────────────────────────────────────────────────────────────

@pytest.fixture
def sample_motor_data() -> List[Dict[str, float]]:
    """Sample motor data for synchronization tests."""
    return [
        {'timestamp_s': 0.001, 'velocity_rad_s': 100.0, 'measured_current_a': 1.0},
        {'timestamp_s': 0.002, 'velocity_rad_s': 101.0, 'measured_current_a': 1.1},
        {'timestamp_s': 0.003, 'velocity_rad_s': 102.0, 'measured_current_a': 1.2},
        {'timestamp_s': 0.004, 'velocity_rad_s': 103.0, 'measured_current_a': 1.3},
        {'timestamp_s': 0.005, 'velocity_rad_s': 104.0, 'measured_current_a': 1.4},
    ]


@pytest.fixture
def sample_sensor_data() -> List[Dict[str, float]]:
    """Sample sensor data for synchronization tests (4800 Hz = ~0.208ms period)."""
    return [
        {'timestamp_s': 0.0005, 'torque_nm': 10.0},
        {'timestamp_s': 0.0010, 'torque_nm': 10.5},
        {'timestamp_s': 0.0015, 'torque_nm': 11.0},
        {'timestamp_s': 0.0020, 'torque_nm': 11.5},
        {'timestamp_s': 0.0025, 'torque_nm': 12.0},
        {'timestamp_s': 0.0030, 'torque_nm': 12.5},
        {'timestamp_s': 0.0035, 'torque_nm': 13.0},
        {'timestamp_s': 0.0040, 'torque_nm': 13.5},
        {'timestamp_s': 0.0045, 'torque_nm': 14.0},
        {'timestamp_s': 0.0050, 'torque_nm': 14.5},
    ]


@pytest.fixture
def sample_psu_data() -> List[Dict[str, float]]:
    """Sample PSU data for synchronization tests (10 Hz = 100ms period)."""
    return [
        {'timestamp_s': 0.0, 'voltage_v': 24.0, 'current_a': 0.5},
        {'timestamp_s': 0.1, 'voltage_v': 24.0, 'current_a': 0.6},
    ]


@pytest.fixture
def sample_synchronized_data() -> List[Dict[str, Any]]:
    """Sample synchronized data for state machine tests."""
    return [
        {
            'timestamp_s': 0.0,
            'motor_velocity_rad_s': 100.0,
            'motor_measured_current_a': 0.0,
            'sensor_torque_nm': 0.0,
            'psu_voltage_v': 24.0,
            'psu_current_a': 0.5
        },
        {
            'timestamp_s': 1.0,
            'motor_velocity_rad_s': 110.0,
            'motor_measured_current_a': 3.4,
            'sensor_torque_nm': 50.0,
            'psu_voltage_v': 24.0,
            'psu_current_a': 1.0
        },
        {
            'timestamp_s': 2.0,
            'motor_velocity_rad_s': 120.0,
            'motor_measured_current_a': 6.8,
            'sensor_torque_nm': 100.0,
            'psu_voltage_v': 24.0,
            'psu_current_a': 1.5
        },
        {
            'timestamp_s': 3.0,
            'motor_velocity_rad_s': 130.0,
            'motor_measured_current_a': 10.2,
            'sensor_torque_nm': 150.0,  # Target torque reached!
            'psu_voltage_v': 24.0,
            'psu_current_a': 2.0
        },
        {
            'timestamp_s': 4.0,
            'motor_velocity_rad_s': 130.0,
            'motor_measured_current_a': 10.2,
            'sensor_torque_nm': 150.0,
            'psu_voltage_v': 24.0,
            'psu_current_a': 2.0
        }
    ]
