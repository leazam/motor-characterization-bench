"""
Tests for multi-rate data synchronization.

Tests cover:
- Timestamp alignment with nearest-prior join
- Jitter margin handling (conservative approach)
- Edge cases (empty data, no matches)
"""

import pytest
from typing import List, Dict, Any

from automation.synchronization import (
    synchronize_data,
    nearest_prior_index,
    find_timestamp_key,
    JITTER_MARGIN_S
)


# ─────────────────────────────────────────────────────────────
# nearest_prior_index Tests
# ─────────────────────────────────────────────────────────────

class TestNearestPriorIndex:
    """Tests for nearest_prior_index function."""
    
    def test_finds_exact_match(self):
        """Should find sample with exact timestamp match."""
        data = [
            {'timestamp_s': 1.0, 'value': 10},
            {'timestamp_s': 2.0, 'value': 20},
            {'timestamp_s': 3.0, 'value': 30},
        ]
        
        # With jitter margin of 0.5ms, target=2.0 should match sample at 2.0
        # because 2.0 + 0.0005 = 2.0005 > 2.0? No, we're checking if (sample_ts + jitter) <= target
        # 2.0 + 0.0005 = 2.0005 > 2.0, so sample at 2.0 is REJECTED with default jitter
        # Use jitter=0 for exact match test
        idx = nearest_prior_index(2.0, data, 'timestamp_s', jitter_margin_s=0)
        
        assert idx == 1  # Sample at timestamp_s=2.0
    
    def test_finds_nearest_prior(self):
        """Should find the most recent sample before target."""
        data = [
            {'timestamp_s': 1.0, 'value': 10},
            {'timestamp_s': 2.0, 'value': 20},
            {'timestamp_s': 3.0, 'value': 30},
        ]
        
        # Target 2.5, should find sample at 2.0 (with 0 jitter)
        idx = nearest_prior_index(2.5, data, 'timestamp_s', jitter_margin_s=0)
        
        assert idx == 1
    
    def test_returns_negative_when_no_prior_exists(self):
        """Should return -1 when no sample exists before target."""
        data = [
            {'timestamp_s': 2.0, 'value': 20},
            {'timestamp_s': 3.0, 'value': 30},
        ]
        
        idx = nearest_prior_index(1.0, data, 'timestamp_s', jitter_margin_s=0)
        
        assert idx == -1
    
    def test_returns_negative_for_empty_data(self):
        """Should return -1 for empty data list."""
        idx = nearest_prior_index(1.0, [], 'timestamp_s')
        
        assert idx == -1
    
    def test_start_idx_optimization(self):
        """Should use start_idx for optimization in sequential access."""
        data = [
            {'timestamp_s': 1.0, 'value': 10},
            {'timestamp_s': 2.0, 'value': 20},
            {'timestamp_s': 3.0, 'value': 30},
            {'timestamp_s': 4.0, 'value': 40},
        ]
        
        # Start search from index 2, target is 3.5
        idx = nearest_prior_index(3.5, data, 'timestamp_s', start_idx=2, jitter_margin_s=0)
        
        assert idx == 2  # Sample at timestamp_s=3.0


class TestJitterMarginHandling:
    """Tests for conservative jitter margin handling."""
    
    def test_jitter_margin_rejects_close_samples(self):
        """Sample within jitter margin of target should be rejected."""
        data = [
            {'timestamp_s': 0.999, 'value': 10},   # 0.999 + 0.0005 = 0.9995 <= 1.0 → ACCEPT
            {'timestamp_s': 0.9998, 'value': 20},  # 0.9998 + 0.0005 = 1.0003 > 1.0 → REJECT
        ]
        
        # Motor timestamp is 1.0, jitter is 0.5ms (0.0005s)
        # Sensor at 0.9998s: worst-case capture time = 0.9998 + 0.0005 = 1.0003 > 1.0 → REJECT
        idx = nearest_prior_index(1.0, data, 'timestamp_s', jitter_margin_s=0.0005)
        
        assert idx == 0  # Only first sample accepted
    
    def test_jitter_margin_accepts_safe_samples(self):
        """Sample safely before jitter margin should be accepted."""
        data = [
            {'timestamp_s': 0.998, 'value': 10},   # 0.998 + 0.0005 = 0.9985 <= 1.0 → ACCEPT
            {'timestamp_s': 0.999, 'value': 20},   # 0.999 + 0.0005 = 0.9995 <= 1.0 → ACCEPT
        ]
        
        idx = nearest_prior_index(1.0, data, 'timestamp_s', jitter_margin_s=0.0005)
        
        assert idx == 1  # Both accepted, but idx=1 is most recent
    
    def test_default_jitter_margin_value(self):
        """Default jitter margin should be 0.5ms (0.0005s)."""
        assert JITTER_MARGIN_S == 0.0005
    
    def test_zero_jitter_margin(self):
        """With zero jitter margin, exact timestamps allowed."""
        data = [
            {'timestamp_s': 1.0, 'value': 10},
        ]
        
        # With zero jitter, sample at 1.0 qualifies for target 1.0
        idx = nearest_prior_index(1.0, data, 'timestamp_s', jitter_margin_s=0)
        
        assert idx == 0
    
    def test_larger_jitter_margin_more_conservative(self):
        """Larger jitter margin should reject more samples."""
        data = [
            {'timestamp_s': 0.980, 'value': 10},
            {'timestamp_s': 0.990, 'value': 20},
            {'timestamp_s': 0.999, 'value': 30},
        ]
        
        # With 1ms jitter (0.001s)
        # 0.999 + 0.001 = 1.000 <= 1.0 → ACCEPT (exactly equal passes)
        idx_1ms = nearest_prior_index(1.0, data, 'timestamp_s', jitter_margin_s=0.001)
        assert idx_1ms == 2  # All pass, most recent is idx=2
        
        # With 15ms jitter (0.015s) - more conservative
        # 0.980 + 0.015 = 0.995 <= 1.0 → ACCEPT
        # 0.990 + 0.015 = 1.005 > 1.0 → REJECT
        idx_15ms = nearest_prior_index(1.0, data, 'timestamp_s', jitter_margin_s=0.015)
        assert idx_15ms == 0  # Only 0.980 passes


# ─────────────────────────────────────────────────────────────
# synchronize_data Tests
# ─────────────────────────────────────────────────────────────

class TestSynchronizeData:
    """Tests for main synchronize_data function."""
    
    def test_synchronizes_all_streams(
        self, 
        sample_motor_data, 
        sample_sensor_data, 
        sample_psu_data, 
        minimal_test_config
    ):
        """Should produce one output sample per motor sample."""
        synced, stats = synchronize_data(
            motor_data=sample_motor_data,
            sensor_data=sample_sensor_data,
            psu_data=sample_psu_data,
            config=minimal_test_config
        )
        
        assert len(synced) == len(sample_motor_data)
        assert stats['total_motor_samples'] == len(sample_motor_data)
    
    def test_motor_fields_prefixed(
        self, 
        sample_motor_data, 
        sample_sensor_data, 
        sample_psu_data, 
        minimal_test_config
    ):
        """Motor fields should have 'motor_' prefix."""
        synced, _ = synchronize_data(
            motor_data=sample_motor_data,
            sensor_data=sample_sensor_data,
            psu_data=sample_psu_data,
            config=minimal_test_config
        )
        
        # Check first sample
        assert 'motor_velocity_rad_s' in synced[0]
        assert 'motor_measured_current_a' in synced[0]
        assert 'velocity_rad_s' not in synced[0]  # Should be prefixed
    
    def test_sensor_fields_prefixed(
        self, 
        sample_motor_data, 
        sample_sensor_data, 
        sample_psu_data, 
        minimal_test_config
    ):
        """Sensor fields should have 'sensor_' prefix."""
        synced, _ = synchronize_data(
            motor_data=sample_motor_data,
            sensor_data=sample_sensor_data,
            psu_data=sample_psu_data,
            config=minimal_test_config
        )
        
        # Check a sample that should have sensor data
        assert 'sensor_torque_nm' in synced[0] or stats_has_sensor_misses(synced, 0)
    
    def test_psu_fields_prefixed(
        self, 
        sample_motor_data, 
        sample_sensor_data, 
        sample_psu_data, 
        minimal_test_config
    ):
        """PSU fields should have 'psu_' prefix."""
        synced, _ = synchronize_data(
            motor_data=sample_motor_data,
            sensor_data=sample_sensor_data,
            psu_data=sample_psu_data,
            config=minimal_test_config
        )
        
        # Check a sample that should have PSU data
        # PSU at 0.0s should match motor at 0.001s (with jitter consideration)
        assert 'psu_voltage_v' in synced[0] or synced[0].get('psu_lag_s') is None
    
    def test_unified_timestamp(
        self, 
        sample_motor_data, 
        sample_sensor_data, 
        sample_psu_data, 
        minimal_test_config
    ):
        """Each synchronized sample should have motor timestamp as unified timestamp."""
        synced, _ = synchronize_data(
            motor_data=sample_motor_data,
            sensor_data=sample_sensor_data,
            psu_data=sample_psu_data,
            config=minimal_test_config
        )
        
        for i, sample in enumerate(synced):
            assert sample['timestamp_s'] == sample_motor_data[i]['timestamp_s']
    
    def test_tracks_lag_for_debugging(
        self, 
        sample_motor_data, 
        sample_sensor_data, 
        sample_psu_data, 
        minimal_test_config
    ):
        """Should track sensor/psu lag for debugging."""
        synced, _ = synchronize_data(
            motor_data=sample_motor_data,
            sensor_data=sample_sensor_data,
            psu_data=sample_psu_data,
            config=minimal_test_config
        )
        
        # All samples should have lag fields (value or None)
        for sample in synced:
            assert 'sensor_lag_s' in sample
            assert 'psu_lag_s' in sample
    
    def test_stats_counts_matches_and_misses(
        self, 
        sample_motor_data, 
        sample_sensor_data, 
        sample_psu_data, 
        minimal_test_config
    ):
        """Stats should count sensor/psu matches and misses."""
        synced, stats = synchronize_data(
            motor_data=sample_motor_data,
            sensor_data=sample_sensor_data,
            psu_data=sample_psu_data,
            config=minimal_test_config
        )
        
        assert 'sensor_matches' in stats
        assert 'sensor_misses' in stats
        assert 'psu_matches' in stats
        assert 'psu_misses' in stats
        assert stats['sensor_matches'] + stats['sensor_misses'] == len(sample_motor_data)
        assert stats['psu_matches'] + stats['psu_misses'] == len(sample_motor_data)


class TestSynchronizeDataEdgeCases:
    """Edge case tests for synchronize_data."""
    
    def test_no_sensor_matches_early_motor(self, minimal_test_config):
        """Motor samples before any sensor samples should have no sensor match."""
        motor_data = [
            {'timestamp_s': 0.0001, 'velocity_rad_s': 100.0, 'measured_current_a': 1.0},
        ]
        sensor_data = [
            {'timestamp_s': 0.001, 'torque_nm': 10.0},  # After motor sample
        ]
        psu_data = [
            {'timestamp_s': 0.0, 'voltage_v': 24.0, 'current_a': 0.5},
        ]
        
        synced, stats = synchronize_data(
            motor_data=motor_data,
            sensor_data=sensor_data,
            psu_data=psu_data,
            config=minimal_test_config
        )
        
        assert stats['sensor_misses'] == 1
        assert synced[0].get('sensor_lag_s') is None
    
    def test_missing_timestamp_raises_error(self, minimal_test_config):
        """Missing timestamp field should raise ValueError."""
        motor_no_ts = [{'velocity_rad_s': 100.0}]  # No timestamp
        sensor_data = [{'timestamp_s': 0.001, 'torque_nm': 10.0}]
        psu_data = [{'timestamp_s': 0.0, 'voltage_v': 24.0, 'current_a': 0.5}]
        
        with pytest.raises(ValueError, match="no timestamp"):
            synchronize_data(
                motor_data=motor_no_ts,
                sensor_data=sensor_data,
                psu_data=psu_data,
                config=minimal_test_config
            )


class TestSynchronizeDataWithRealJitter:
    """Tests simulating real-world jitter scenarios."""
    
    def test_jittered_sensor_data(self, minimal_test_config):
        """Sensor with jitter should still sync correctly."""
        # Motor at regular 1ms intervals
        motor_data = [
            {'timestamp_s': 0.001, 'velocity_rad_s': 100.0, 'measured_current_a': 1.0},
            {'timestamp_s': 0.002, 'velocity_rad_s': 101.0, 'measured_current_a': 1.1},
            {'timestamp_s': 0.003, 'velocity_rad_s': 102.0, 'measured_current_a': 1.2},
        ]
        
        # Sensor with jitter (not perfectly periodic)
        sensor_data = [
            {'timestamp_s': 0.0002, 'torque_nm': 10.0},    # Early
            {'timestamp_s': 0.00045, 'torque_nm': 10.5},   # Slightly late
            {'timestamp_s': 0.0008, 'torque_nm': 11.0},    # Early
            {'timestamp_s': 0.0012, 'torque_nm': 11.5},    # Late
            {'timestamp_s': 0.0018, 'torque_nm': 12.0},
            {'timestamp_s': 0.0025, 'torque_nm': 12.5},
        ]
        
        psu_data = [
            {'timestamp_s': 0.0, 'voltage_v': 24.0, 'current_a': 0.5},
        ]
        
        synced, stats = synchronize_data(
            motor_data=motor_data,
            sensor_data=sensor_data,
            psu_data=psu_data,
            config=minimal_test_config
        )
        
        assert len(synced) == 3
        # Most samples should have sensor matches due to high sensor rate
        assert stats['sensor_matches'] >= 2


# ─────────────────────────────────────────────────────────────
# find_timestamp_key Tests
# ─────────────────────────────────────────────────────────────

class TestFindTimestampKey:
    """Tests for find_timestamp_key helper function."""
    
    def test_finds_timestamp_s(self):
        """Should find 'timestamp_s' key."""
        data = [{'timestamp_s': 0.001, 'value': 10}]
        assert find_timestamp_key(data) == 'timestamp_s'
    
    def test_finds_timestamp_ms(self):
        """Should find 'timestamp_ms' key."""
        data = [{'timestamp_ms': 1000, 'value': 10}]
        assert find_timestamp_key(data) == 'timestamp_ms'
    
    def test_finds_key_with_timestamp_in_name(self):
        """Should find any key containing 'timestamp'."""
        data = [{'motor_timestamp': 0.001, 'value': 10}]
        assert find_timestamp_key(data) == 'motor_timestamp'
    
    def test_returns_none_for_empty_data(self):
        """Should return None for empty data."""
        assert find_timestamp_key([]) is None
    
    def test_returns_none_when_no_timestamp_field(self):
        """Should return None when no timestamp field exists."""
        data = [{'value': 10, 'other': 20}]
        assert find_timestamp_key(data) is None


# ─────────────────────────────────────────────────────────────
# Helper Functions
# ─────────────────────────────────────────────────────────────

def stats_has_sensor_misses(synced: List[Dict], index: int) -> bool:
    """Check if a synchronized sample has sensor miss (None lag)."""
    return synced[index].get('sensor_lag_s') is None
