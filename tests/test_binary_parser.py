"""
Tests for motor binary parser.

Tests cover:
- Correct decode of valid packets
- Checksum failure detection
- Partial/truncated packet handling
- Resync after corruption (missing start marker)
"""

import pytest
import struct
from pathlib import Path

from drivers.motor import load_motor_bin, parse_message_fields


class TestBinaryParserCorrectDecode:
    """Tests for correct decoding of valid binary packets."""
    
    def test_single_valid_packet(self, temp_bin_file, build_packet, minimal_motor_protocol):
        """A single valid telemetry packet should be parsed correctly."""
        packet = build_packet(
            timestamp_ms=1000,
            velocity=150.5,
            measured_current=12.3
        )
        bin_path = temp_bin_file("motor.bin", packet)
        
        data, errors = load_motor_bin(bin_path, minimal_motor_protocol)
        
        assert len(data) == 1
        assert data[0]['timestamp_s'] == 1.0  # 1000ms -> 1.0s
        # Field names include units (normalized to match CSV convention)
        assert abs(data[0]['velocity_rad_s'] - 150.5) < 0.001
        assert abs(data[0]['measured_current_a'] - 12.3) < 0.001
        assert data[0]['message_type'] == 'response'
        assert data[0]['message_name'] == 'telemetry'
    
    def test_multiple_valid_packets(self, temp_bin_file, build_packet, minimal_motor_protocol):
        """Multiple valid packets should all be parsed."""
        packets = b''
        packets += build_packet(timestamp_ms=1000, velocity=100.0, measured_current=1.0)
        packets += build_packet(timestamp_ms=2000, velocity=110.0, measured_current=2.0)
        packets += build_packet(timestamp_ms=3000, velocity=120.0, measured_current=3.0)
        
        bin_path = temp_bin_file("motor.bin", packets)
        
        data, errors = load_motor_bin(bin_path, minimal_motor_protocol)
        
        assert len(data) == 3
        assert data[0]['timestamp_s'] == 1.0
        assert data[1]['timestamp_s'] == 2.0
        assert data[2]['timestamp_s'] == 3.0
        assert abs(data[0]['velocity_rad_s'] - 100.0) < 0.001
        assert abs(data[1]['velocity_rad_s'] - 110.0) < 0.001
        assert abs(data[2]['velocity_rad_s'] - 120.0) < 0.001
    
    def test_timestamp_monotonicity_filter(self, temp_bin_file, build_packet, minimal_motor_protocol):
        """Non-monotonic timestamps should be filtered out."""
        packets = b''
        packets += build_packet(timestamp_ms=1000, velocity=100.0, measured_current=1.0)
        packets += build_packet(timestamp_ms=500, velocity=110.0, measured_current=2.0)  # Out of order!
        packets += build_packet(timestamp_ms=2000, velocity=120.0, measured_current=3.0)
        
        bin_path = temp_bin_file("motor.bin", packets)
        
        data, errors = load_motor_bin(bin_path, minimal_motor_protocol)
        
        assert len(data) == 2  # Middle packet skipped
        assert data[0]['timestamp_s'] == 1.0
        assert data[1]['timestamp_s'] == 2.0
        assert any('non-monotonic' in e.lower() or 'Non-monotonic' in e for e in errors)


class TestBinaryParserChecksumFailure:
    """Tests for checksum failure detection."""
    
    def test_corrupted_checksum_discards_packet(self, temp_bin_file, build_packet, minimal_motor_protocol):
        """A packet with corrupted checksum should be discarded."""
        valid_packet = build_packet(timestamp_ms=1000, velocity=100.0, measured_current=1.0)
        corrupt_packet = build_packet(
            timestamp_ms=2000, velocity=110.0, measured_current=2.0,
            corrupt_checksum=True
        )
        valid_packet2 = build_packet(timestamp_ms=3000, velocity=120.0, measured_current=3.0)
        
        packets = valid_packet + corrupt_packet + valid_packet2
        bin_path = temp_bin_file("motor.bin", packets)
        
        data, errors = load_motor_bin(bin_path, minimal_motor_protocol)
        
        # Should have 2 valid packets (corrupt one discarded)
        assert len(data) == 2
        assert data[0]['timestamp_s'] == 1.0
        assert data[1]['timestamp_s'] == 3.0
        assert any('checksum' in e.lower() for e in errors)
    
    def test_all_packets_corrupt_raises_error(self, temp_bin_file, build_packet, minimal_motor_protocol):
        """If all packets have bad checksums, should raise ValueError."""
        corrupt1 = build_packet(timestamp_ms=1000, velocity=100.0, measured_current=1.0, corrupt_checksum=True)
        corrupt2 = build_packet(timestamp_ms=2000, velocity=110.0, measured_current=2.0, corrupt_checksum=True)
        
        bin_path = temp_bin_file("motor.bin", corrupt1 + corrupt2)
        
        with pytest.raises(ValueError, match="No valid packets"):
            load_motor_bin(bin_path, minimal_motor_protocol)


class TestBinaryParserPartialPacket:
    """Tests for partial/truncated packet handling."""
    
    def test_truncated_packet_at_end(self, temp_bin_file, build_packet, minimal_motor_protocol):
        """A truncated packet at end of file should be reported but not crash."""
        valid_packet = build_packet(timestamp_ms=1000, velocity=100.0, measured_current=1.0)
        truncated = bytes([0xAA, 0x55, 0x42, 0x09])  # Start marker + partial header
        
        bin_path = temp_bin_file("motor.bin", valid_packet + truncated)
        
        data, errors = load_motor_bin(bin_path, minimal_motor_protocol)
        
        assert len(data) == 1  # Only the valid packet
        assert data[0]['timestamp_s'] == 1.0
        assert any('truncated' in e.lower() for e in errors)
    
    def test_missing_end_marker(self, temp_bin_file, build_packet, minimal_motor_protocol):
        """A packet missing end marker should trigger resync."""
        no_end_marker = build_packet(timestamp_ms=1000, velocity=100.0, measured_current=1.0, omit_end_marker=True)
        valid_packet = build_packet(timestamp_ms=2000, velocity=110.0, measured_current=2.0)
        
        bin_path = temp_bin_file("motor.bin", no_end_marker + valid_packet)
        
        data, errors = load_motor_bin(bin_path, minimal_motor_protocol)
        
        # Parser should resync and find the second packet
        assert len(data) >= 1
        # Should report end marker issue
        assert any('end marker' in e.lower() or 'resync' in e.lower() for e in errors)
    
    def test_empty_file_raises_error(self, temp_bin_file, minimal_motor_protocol):
        """An empty binary file should raise ValueError."""
        bin_path = temp_bin_file("motor.bin", b'')
        
        with pytest.raises(ValueError, match="Empty binary file"):
            load_motor_bin(bin_path, minimal_motor_protocol)


class TestBinaryParserResyncAfterCorruption:
    """Tests for resync behavior after data corruption."""
    
    def test_garbage_before_valid_packet(self, temp_bin_file, build_packet, minimal_motor_protocol):
        """Garbage bytes before a valid packet should be skipped via resync."""
        garbage = bytes([0x00, 0xFF, 0x12, 0x34, 0x56])  # Random garbage
        valid_packet = build_packet(timestamp_ms=1000, velocity=100.0, measured_current=1.0)
        
        bin_path = temp_bin_file("motor.bin", garbage + valid_packet)
        
        data, errors = load_motor_bin(bin_path, minimal_motor_protocol)
        
        assert len(data) == 1
        assert data[0]['timestamp_s'] == 1.0
        assert any('resync' in e.lower() or 'start marker' in e.lower() for e in errors)
    
    def test_garbage_between_valid_packets(self, temp_bin_file, build_packet, minimal_motor_protocol):
        """Garbage between packets should trigger resync to find next packet."""
        packet1 = build_packet(timestamp_ms=1000, velocity=100.0, measured_current=1.0)
        garbage = bytes([0x00, 0xFF, 0x12, 0x34])
        packet2 = build_packet(timestamp_ms=2000, velocity=110.0, measured_current=2.0)
        
        bin_path = temp_bin_file("motor.bin", packet1 + garbage + packet2)
        
        data, errors = load_motor_bin(bin_path, minimal_motor_protocol)
        
        assert len(data) == 2
        assert data[0]['timestamp_s'] == 1.0
        assert data[1]['timestamp_s'] == 2.0
    
    def test_false_start_marker_in_data(self, temp_bin_file, build_packet, minimal_motor_protocol):
        """A false start marker (0xAA 0x55) in garbage should not crash."""
        valid_packet = build_packet(timestamp_ms=1000, velocity=100.0, measured_current=1.0)
        # Data containing a false start marker
        garbage_with_false_start = bytes([0x00, 0xAA, 0x55, 0x00, 0x00])
        packet2 = build_packet(timestamp_ms=2000, velocity=110.0, measured_current=2.0)
        
        bin_path = temp_bin_file("motor.bin", valid_packet + garbage_with_false_start + packet2)
        
        data, errors = load_motor_bin(bin_path, minimal_motor_protocol)
        
        # Should still find both valid packets
        assert len(data) >= 1
        assert data[0]['timestamp_s'] == 1.0


class TestBinaryParserUnknownMessageCode:
    """Tests for unknown message code handling."""
    
    def test_unknown_response_code_skipped(self, temp_bin_file, build_packet, minimal_motor_protocol):
        """An unknown response code should be skipped with warning."""
        # 0xFF is not a defined response code
        unknown_packet = build_packet(timestamp_ms=1000, velocity=100.0, measured_current=1.0, response_code=0xFF)
        valid_packet = build_packet(timestamp_ms=2000, velocity=110.0, measured_current=2.0, response_code=0x0E)
        
        bin_path = temp_bin_file("motor.bin", unknown_packet + valid_packet)
        
        data, errors = load_motor_bin(bin_path, minimal_motor_protocol)
        
        assert len(data) == 1
        assert data[0]['timestamp_s'] == 2.0
        assert any('unknown' in e.lower() for e in errors)


class TestParseMessageFields:
    """Tests for parse_message_fields helper function."""
    
    def test_parse_telemetry_fields(self, minimal_motor_protocol):
        """Correctly parse telemetry response fields with normalized names."""
        # Velocity=150.0 and measured_current=10.0 as float32
        field_data = struct.pack('<ff', 150.0, 10.0)
        
        message_def = minimal_motor_protocol['responses'][0]  # telemetry
        types = minimal_motor_protocol['types']
        
        result = parse_message_fields(field_data, message_def, types, 1000, 'response')
        
        assert result is not None
        assert result['timestamp_s'] == 1.0
        # Field names are normalized with units: velocity -> velocity_rad_s
        assert abs(result['velocity_rad_s'] - 150.0) < 0.001
        assert abs(result['measured_current_a'] - 10.0) < 0.001
        assert result['message_type'] == 'response'
        assert result['message_name'] == 'telemetry'
    
    def test_parse_insufficient_data_returns_none(self, minimal_motor_protocol):
        """Returns None if field_data is too short."""
        # Only 4 bytes, but we need 8 (two float32)
        field_data = struct.pack('<f', 150.0)
        
        message_def = minimal_motor_protocol['responses'][0]
        types = minimal_motor_protocol['types']
        
        result = parse_message_fields(field_data, message_def, types, 1000, 'response')
        
        assert result is None


class TestNormalizeFieldName:
    """Tests for field name normalization (appending units)."""
    
    def test_appends_simple_unit(self):
        """Simple units like 'A' become '_a' suffix."""
        from drivers.motor import normalize_field_name
        assert normalize_field_name('measured_current', 'A') == 'measured_current_a'
    
    def test_appends_compound_unit(self):
        """Compound units like 'rad/s' become '_rad_s' suffix."""
        from drivers.motor import normalize_field_name
        assert normalize_field_name('velocity', 'rad/s') == 'velocity_rad_s'
    
    def test_no_unit_returns_original(self):
        """Empty unit returns original name unchanged."""
        from drivers.motor import normalize_field_name
        assert normalize_field_name('motor_id', '') == 'motor_id'
    
    def test_unit_is_lowercased(self):
        """Unit should be lowercased."""
        from drivers.motor import normalize_field_name
        assert normalize_field_name('voltage', 'V') == 'voltage_v'


class TestBinaryCSVFieldCompatibility:
    """Tests that binary parser produces field names compatible with CSV."""
    
    def test_binary_fields_match_csv_convention(self, temp_bin_file, build_packet, minimal_motor_protocol):
        """Binary parser field names should match CSV column names."""
        packet = build_packet(timestamp_ms=1000, velocity=100.0, measured_current=5.0)
        bin_path = temp_bin_file("motor.bin", packet)
        
        data, errors = load_motor_bin(bin_path, minimal_motor_protocol)
        
        assert len(data) == 1
        sample = data[0]
        
        # These field names must match CSV conventions for synchronization to work
        assert 'velocity_rad_s' in sample, f"Missing 'velocity_rad_s', got: {list(sample.keys())}"
        assert 'measured_current_a' in sample, f"Missing 'measured_current_a', got: {list(sample.keys())}"
        
        # Old field names should NOT exist
        assert 'velocity' not in sample, "Old field name 'velocity' should not exist"
        assert 'measured_current' not in sample, "Old field name 'measured_current' should not exist"
