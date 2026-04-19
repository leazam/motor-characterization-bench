"""
Multi-rate data synchronization.

Aligns data from different instruments (motor @ 1000Hz, sensor @ 4800Hz, 
PSU @ 10Hz) using the synchronization method defined in test_config.yaml.

Jitter Handling (CONSERVATIVE approach):
- Motor is the PRIMARY CLOCK (reference, accurate timestamps)
- Sensor/PSU timestamps have scheduling jitter (±20µs typical, up to ±0.5ms)
- We ADD jitter margin to sensor/PSU timestamps (they have the jitter, not motor)
- Condition: (sensor_ts + jitter_margin) <= motor_ts
- Example: sensor at 0.9998s + 0.5ms jitter = 1.0003s > motor at 1.000s → REJECT
- This may miss valid samples, but NEVER uses samples captured after motor_ts
"""

import logging
from typing import Dict, Any, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Jitter margin: sensor/PSU timestamps can vary by up to ±0.5ms due to scheduling jitter
# We ADD this to sensor/PSU timestamps to get worst-case actual capture time
JITTER_MARGIN_S = 0.0005  # 0.5 milliseconds


def find_timestamp_key(data: List[Dict[str, Any]]) -> Optional[str]:
    """
    Find the timestamp field name in a data sample.
    
    Args:
        data: List of sample dictionaries
        
    Returns:
        Timestamp field name, or None if not found
    """
    if not data:
        return None
    
    sample = data[0]
    
    # Check common names first
    for key in ['timestamp_s', 'timestamp_ms']:
        if key in sample:
            return key
    
    # Fallback: look for any key containing 'timestamp'
    for key in sample.keys():
        if 'timestamp' in key.lower():
            return key
    
    return None


def nearest_prior_index(
    target_ts: float,
    data: List[Dict[str, Any]],
    ts_key: str,
    start_idx: int = 0,
    jitter_margin_s: float = JITTER_MARGIN_S
) -> int:
    """
    Find index of the most recent sample where (timestamp + jitter) <= target_ts.
    
    CONSERVATIVE approach: Sensor/PSU timestamps have jitter. A sample with recorded
    timestamp 0.9998s could ACTUALLY have been captured at 1.0003s (worst-case +0.5ms).
    We ADD jitter to the sensor/PSU timestamp to get worst-case actual capture time,
    and only accept if that's still <= motor_ts.
    
    Condition: (sensor_ts + jitter_margin) <= motor_ts
    
    Example:
        Motor ts: 1.000s, jitter: 0.5ms
        Sensor at 0.999s  → 0.999 + 0.0005 = 0.9995 <= 1.000 → ACCEPT ✓
        Sensor at 0.9998s → 0.9998 + 0.0005 = 1.0003 > 1.000 → REJECT ✗
    
    Uses linear scan from start_idx (optimized for sequential access).
    
    Args:
        target_ts: Motor timestamp (primary clock, reference, accurate)
        data: Sensor or PSU samples (sorted by timestamp, affected by jitter)
        ts_key: Name of timestamp field
        start_idx: Index to start searching from (optimization for sequential calls)
        jitter_margin_s: Max jitter in sensor/PSU timestamps (default: 0.5ms)
        
    Returns:
        Index of nearest prior sample, or -1 if no valid sample found
    """
    if not data:
        return -1
    
    # Check if first sample (with worst-case jitter) is already past motor_ts
    if (data[0][ts_key] + jitter_margin_s) > target_ts:
        return -1
    
    # Start from start_idx, but ensure it's valid
    result_idx = -1
    if start_idx < len(data) and (data[start_idx][ts_key] + jitter_margin_s) <= target_ts:
        result_idx = start_idx
    elif (data[0][ts_key] + jitter_margin_s) <= target_ts:
        result_idx = 0
    
    # Scan forward from start_idx, find latest sample where (ts + jitter) <= motor_ts
    for i in range(max(start_idx, 0), len(data)):
        sample_ts = data[i][ts_key]
        # Add jitter to sensor/PSU timestamp (they have the jitter, not motor)
        if (sample_ts + jitter_margin_s) <= target_ts:
            result_idx = i
        else:
            # This sample's worst-case time is past motor_ts, stop scanning
            break
    
    return result_idx


def synchronize_data(
    motor_data: List[Dict[str, Any]],
    sensor_data: List[Dict[str, Any]],
    psu_data: List[Dict[str, Any]],
    config: Dict[str, Any]
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Synchronize multi-rate data using nearest-prior join.
    
    Motor stream is the primary clock. For each motor sample, finds
    the most recent sensor and PSU samples with timestamp <= motor timestamp.
    
    Args:
        motor_data: Motor samples (primary clock, ~1000 Hz)
        sensor_data: Sensor samples (~4800 Hz)
        psu_data: PSU samples (~10 Hz)
        config: Parsed test_config.yaml
        
    Returns:
        Tuple of (synchronized_data, stats):
        - synchronized_data: List of dicts, one per motor sample, with merged fields
        - stats: Dict with synchronization statistics
    """
    logger.info("Synchronizing data streams (nearest-prior join)...")
    
    # Find timestamp keys for each data source
    motor_ts_key = find_timestamp_key(motor_data)
    sensor_ts_key = find_timestamp_key(sensor_data)
    psu_ts_key = find_timestamp_key(psu_data)
    
    if motor_ts_key is None:
        raise ValueError("Motor data has no timestamp field")
    if sensor_ts_key is None:
        raise ValueError("Sensor data has no timestamp field")
    if psu_ts_key is None:
        raise ValueError("PSU data has no timestamp field")
    
    logger.debug(f"Timestamp keys: motor={motor_ts_key}, sensor={sensor_ts_key}, psu={psu_ts_key}")
    
    synchronized = []
    stats = {
        'total_motor_samples': len(motor_data),
        'sensor_matches': 0,
        'sensor_misses': 0,
        'psu_matches': 0,
        'psu_misses': 0
    }
    
    # Track last matched indices for optimization (sequential access pattern)
    last_sensor_idx = 0
    last_psu_idx = 0
    
    for motor_sample in motor_data:
        motor_ts = motor_sample[motor_ts_key]
        
        # Create synchronized sample starting with motor data
        sync_sample = {
            'timestamp_s': motor_ts,  # Unified timestamp
        }
        
        # Copy motor fields (prefix to avoid collisions)
        for key, value in motor_sample.items():
            if key != motor_ts_key:
                sync_sample[f'motor_{key}'] = value
        
        # Find nearest prior sensor sample
        sensor_idx = nearest_prior_index(motor_ts, sensor_data, sensor_ts_key, last_sensor_idx)
        if sensor_idx >= 0:
            last_sensor_idx = sensor_idx
            sensor_sample = sensor_data[sensor_idx]
            stats['sensor_matches'] += 1
            
            # Copy sensor fields (prefix to avoid collisions)
            for key, value in sensor_sample.items():
                if key != sensor_ts_key:
                    sync_sample[f'sensor_{key}'] = value
            
            # Track lag for debugging
            sync_sample['sensor_lag_s'] = motor_ts - sensor_sample[sensor_ts_key]
        else:
            # No valid sensor sample found for this motor timestamp (considering jitter), sensor_idx = -1
            stats['sensor_misses'] += 1
            sync_sample['sensor_lag_s'] = None
        
        # Find nearest prior PSU sample
        psu_idx = nearest_prior_index(motor_ts, psu_data, psu_ts_key, last_psu_idx)
        if psu_idx >= 0:
            last_psu_idx = psu_idx
            psu_sample = psu_data[psu_idx]
            stats['psu_matches'] += 1
            
            # Copy PSU fields (prefix to avoid collisions)
            for key, value in psu_sample.items():
                if key != psu_ts_key:
                    sync_sample[f'psu_{key}'] = value
            
            # Track lag for debugging
            sync_sample['psu_lag_s'] = motor_ts - psu_sample[psu_ts_key]
        else:
            # No valid PSU sample found for this motor timestamp (considering jitter), psu_idx = -1
            stats['psu_misses'] += 1
            sync_sample['psu_lag_s'] = None
        
        synchronized.append(sync_sample)

    # Log summary
    logger.info(f"Synchronized {len(synchronized)} samples")
    logger.info(f"  Sensor: {stats['sensor_matches']} matches, {stats['sensor_misses']} misses")
    logger.info(f"  PSU: {stats['psu_matches']} matches, {stats['psu_misses']} misses")
    
    return synchronized, stats
