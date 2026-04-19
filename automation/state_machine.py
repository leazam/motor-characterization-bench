"""
YAML-driven state machine for test phase management.

Phase transitions are based on data timestamps, not wall clock.
All phase names, parameters, and transition logic are read from test_config.yaml.

"""

import logging
import csv
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional

from drivers.motor import load_motor_data
from drivers.sensor import load_sensor_csv
from drivers.psu import load_psu_csv
from automation.synchronization import synchronize_data
from automation.safety import check_safety_limits, check_phase_transition_current_ramp

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# SETUP Phase
# ─────────────────────────────────────────────────────────────

def run_setup_phase(
    motor_data_path: Path,
    sensor_data_path: Path,
    psu_data_path: Path,
    test_config: Dict[str, Any],
    motor_protocol: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Execute SETUP phase: Load and validate all data files.
    
    Reports file stats (row counts, time spans, parse errors).
    
    Args:
        motor_data_path: Path to motor data file (.bin or .csv)
        sensor_data_path: Path to sensor CSV file
        psu_data_path: Path to PSU CSV file
        test_config: Parsed test_config.yaml
        motor_protocol: Parsed motor_protocol.yaml
        
    Returns:
        Dict containing:
        - 'motor_data': List of motor samples
        - 'sensor_data': List of sensor samples
        - 'psu_data': List of PSU samples
        - 'stats': Dict with file statistics
        - 'errors': Dict with parse errors per source
        
    Raises:
        ValueError: If any data file cannot be loaded or has no valid data
    """
    logger.info("SETUP phase: Loading and validating data files...")
    
    result = {
        'motor_data': [],
        'sensor_data': [],
        'psu_data': [],
        'stats': {},
        'errors': {}
    }
    
    # Load motor data
    logger.info(f"Loading motor data from {motor_data_path}")
    motor_data, motor_errors = load_motor_data(motor_data_path, test_config, motor_protocol)
    result['motor_data'] = motor_data
    result['errors']['motor'] = motor_errors
    result['stats']['motor'] = compute_data_stats(motor_data, 'motor')
    
    # Load sensor data
    logger.info(f"Loading sensor data from {sensor_data_path}")
    sensor_data, sensor_errors = load_sensor_csv(sensor_data_path, test_config)
    result['sensor_data'] = sensor_data
    result['errors']['sensor'] = sensor_errors
    result['stats']['sensor'] = compute_data_stats(sensor_data, 'sensor')
    
    # Load PSU data
    logger.info(f"Loading PSU data from {psu_data_path}")
    psu_data, psu_errors = load_psu_csv(psu_data_path, test_config)
    result['psu_data'] = psu_data
    result['errors']['psu'] = psu_errors
    result['stats']['psu'] = compute_data_stats(psu_data, 'psu')
    
    # Log summary report
    log_setup_report(result['stats'], result['errors'])
    
    logger.info("SETUP phase complete")
    return result


def compute_data_stats(data: List[Dict[str, Any]], source_name: str) -> Dict[str, Any]:
    """
    Compute statistics for a loaded data source.
    
    Args:
        data: List of sample dictionaries
        source_name: Name of data source ('motor', 'sensor', 'psu')
        
    Returns:
        Dict with:
        - 'row_count': Number of valid samples
        - 'time_span_s': Duration from first to last timestamp
        - 'first_timestamp_s': First timestamp
        - 'last_timestamp_s': Last timestamp
    """
    if not data:
        return {
            'row_count': 0,
            'time_span_s': 0.0,
            'first_timestamp_s': None,
            'last_timestamp_s': None
        }
    
    # Find timestamp field - check common names
    timestamp_key = None
    for key in ['timestamp_s', 'timestamp_ms']:
        if key in data[0]:
            timestamp_key = key
            break
    
    if timestamp_key is None:
        # Fallback: look for any key containing 'timestamp'
        for key in data[0].keys():
            if 'timestamp' in key.lower():
                timestamp_key = key
                break
    
    if timestamp_key is None:
        logger.warning(f"No timestamp field found in {source_name} data")
        return {
            'row_count': len(data),
            'time_span_s': None,
            'first_timestamp_s': None,
            'last_timestamp_s': None
        }
    
    first_ts = data[0][timestamp_key]
    last_ts = data[-1][timestamp_key]
    
    return {
        'row_count': len(data),
        'time_span_s': last_ts - first_ts,
        'first_timestamp_s': first_ts,
        'last_timestamp_s': last_ts
    }


def log_setup_report(stats: Dict[str, Dict], errors: Dict[str, List[str]]) -> None:
    """
    Log a summary report of loaded data files.
    
    Args:
        stats: Dict of stats per data source
        errors: Dict of error lists per data source
    """
    logger.info("=" * 60)
    logger.info("SETUP Phase Report")
    logger.info("=" * 60)
    
    for source in ['motor', 'sensor', 'psu']:
        source_stats = stats.get(source, {})
        source_errors = errors.get(source, [])
        
        row_count = source_stats.get('row_count', 0)
        time_span = source_stats.get('time_span_s')
        first_ts = source_stats.get('first_timestamp_s')
        last_ts = source_stats.get('last_timestamp_s')
        error_count = len(source_errors)
        
        logger.info(f"\n{source.upper()}:")
        logger.info(f"  Rows:       {row_count}")
        
        if time_span is not None:
            logger.info(f"  Time span:  {time_span:.3f} s ({first_ts:.3f} to {last_ts:.3f})")
        else:
            logger.info(f"  Time span:  N/A")
        
        if error_count > 0:
            logger.warning(f"  Errors:     {error_count}")
        else:
            logger.info(f"  Errors:     0")
    
    logger.info("=" * 60)


# ─────────────────────────────────────────────────────────────
# CURRENT_RAMP Phase
# ─────────────────────────────────────────────────────────────

def run_current_ramp_phase(
    synchronized_data: List[Dict[str, Any]],
    config: Dict[str, Any],
    start_index: int = 0
) -> Dict[str, Any]:
    """
    Execute CURRENT_RAMP phase: Ramp current until torque or current limit.
    
    Tracks commanded current ramping at ramp_rate (derived from max_current / ramp_duration).
    Transitions to TORQUE_HOLD when |torque| >= target_torque OR |current| >= max_current.
    
    Args:
        synchronized_data: List of synchronized samples from synchronize_data()
        config: Parsed test_config.yaml
        start_index: Index to start processing from (0 for fresh start)
        
    Returns:
        Dict containing:
        - 'processed_samples': List of samples with commanded_current_a added
        - 'end_index': Index where phase ended
        - 'next_phase': Name of next phase ('TORQUE_HOLD' or 'COMPLETE')
        - 'transition_reason': Why the phase ended
        - 'data_exhausted': True if ended due to running out of data
        - 'safety_violation': True if ended due to safety limit exceeded
    """
    logger.info("CURRENT_RAMP phase: Starting current ramp...")
    
    # Get phase parameters from YAML
    phases = config['test']['phases']
    ramp_params = None
    for phase in phases:
        if phase['name'] == 'CURRENT_RAMP':
            ramp_params = phase.get('parameters', {})
            break
    
    if ramp_params is None:
        raise ValueError("CURRENT_RAMP parameters not found in config")
    
    max_current = ramp_params.get('max_current_a', 34.0)
    ramp_duration = ramp_params.get('ramp_duration_s', 10.0)
    target_torque = ramp_params.get('target_torque_nm', 150.0)
    
    # Derived: ramp_rate = max_current / ramp_duration
    ramp_rate = max_current / ramp_duration
    
    logger.info(f"  Ramp rate: {ramp_rate:.2f} A/s (max_current={max_current} A, duration={ramp_duration} s)")
    logger.info(f"  Target torque: {target_torque} Nm")
    
    result = {
        'processed_samples': [],
        'end_index': start_index,
        'next_phase': 'TORQUE_HOLD',
        'transition_reason': '',
        'data_exhausted': False,
        'safety_violation': False
    }
    
    if start_index >= len(synchronized_data):
        # No data to process, end phase immediately
        result['next_phase'] = 'COMPLETE'
        result['transition_reason'] = 'No data to process'
        result['data_exhausted'] = True
        logger.warning("CURRENT_RAMP: No data to process")
        return result
    
    # Get start timestamp for ramp calculation
    start_ts = synchronized_data[start_index]['timestamp_s']
    
    # Process samples
    for i in range(start_index, len(synchronized_data)):
        sample = synchronized_data[i].copy()
        current_ts = sample['timestamp_s']
        
        # Calculate commanded current based on elapsed time
        elapsed_s = current_ts - start_ts
        commanded_current = min(elapsed_s * ramp_rate, max_current)
        sample['commanded_current_a'] = commanded_current
        sample['test_phase'] = 'CURRENT_RAMP'
        
        # Check safety limits first
        is_safe, violation_msg = check_safety_limits(sample, config)
        if not is_safe:
            result['end_index'] = i
            result['next_phase'] = 'COMPLETE'
            result['transition_reason'] = violation_msg
            result['safety_violation'] = True
            logger.error(f"CURRENT_RAMP safety violation at index {i}: {violation_msg}")
            return result
        
        # Add sample to processed list
        result['processed_samples'].append(sample)
        result['end_index'] = i + 1
        
        # Check phase transition conditions
        should_transition, reason = check_phase_transition_current_ramp(sample, config)
        if should_transition:
            result['next_phase'] = 'TORQUE_HOLD'
            result['transition_reason'] = reason
            logger.info(f"CURRENT_RAMP completed at index {i}: {reason}")
            return result
    
    # If we get here, data was exhausted before transition
    result['data_exhausted'] = True
    result['next_phase'] = 'COMPLETE'
    result['transition_reason'] = f"Data exhausted during CURRENT_RAMP (processed {len(result['processed_samples'])} samples)"
    logger.warning(result['transition_reason'])
    
    return result


def get_phase_parameters(config: Dict[str, Any], phase_name: str) -> Optional[Dict[str, Any]]:
    """
    Get parameters for a specific phase from config.
    
    Args:
        config: Parsed test_config.yaml
        phase_name: Name of phase (e.g., 'CURRENT_RAMP')
        
    Returns:
        Parameters dict, or None if phase not found
    """
    phases = config['test']['phases']
    for phase in phases:
        if phase['name'] == phase_name:
            return phase.get('parameters', {})
    return None


# ─────────────────────────────────────────────────────────────
# TORQUE_HOLD Phase
# ─────────────────────────────────────────────────────────────

def run_torque_hold_phase(
    synchronized_data: List[Dict[str, Any]],
    config: Dict[str, Any],
    hold_current_a: float,
    start_index: int = 0
) -> Dict[str, Any]:
    """
    Execute TORQUE_HOLD phase: Hold commanded current constant.
    
    Holds commanded current at the value reached during CURRENT_RAMP.
    Continues reading and logging all streams.
    Transitions to VOLTAGE_DECREASE after hold_duration_s of data timestamps.
    
    Args:
        synchronized_data: List of synchronized samples from synchronize_data()
        config: Parsed test_config.yaml
        hold_current_a: Commanded current to hold (from end of CURRENT_RAMP)
        start_index: Index to start processing from
        
    Returns:
        Dict containing:
        - 'processed_samples': List of samples with commanded_current_a added
        - 'end_index': Index where phase ended
        - 'next_phase': Name of next phase ('VOLTAGE_DECREASE' or 'COMPLETE')
        - 'transition_reason': Why the phase ended
        - 'data_exhausted': True if ended due to running out of data
        - 'safety_violation': True if ended due to safety limit exceeded
    """
    logger.info("TORQUE_HOLD phase: Holding current constant...")
    
    # Get phase parameters from YAML
    hold_params = get_phase_parameters(config, 'TORQUE_HOLD')
    
    if hold_params is None:
        raise ValueError("TORQUE_HOLD parameters not found in config")
    
    hold_duration_s = hold_params.get('hold_duration_s', 10.0)
    
    logger.info(f"  Hold current: {hold_current_a:.2f} A")
    logger.info(f"  Hold duration: {hold_duration_s} s")
    
    result = {
        'processed_samples': [],
        'end_index': start_index,
        'next_phase': 'VOLTAGE_DECREASE',
        'transition_reason': '',
        'data_exhausted': False,
        'safety_violation': False
    }
    
    if start_index >= len(synchronized_data):
        result['next_phase'] = 'COMPLETE'
        result['transition_reason'] = 'No data to process'
        result['data_exhausted'] = True
        logger.warning("TORQUE_HOLD: No data to process")
        return result
    
    # Get start timestamp for hold duration calculation
    start_ts = synchronized_data[start_index]['timestamp_s']
    
    # Process samples
    for i in range(start_index, len(synchronized_data)):
        sample = synchronized_data[i].copy()
        current_ts = sample['timestamp_s']
        
        # Commanded current stays constant at hold value
        sample['commanded_current_a'] = hold_current_a
        sample['test_phase'] = 'TORQUE_HOLD'
        
        # Check safety limits first
        is_safe, violation_msg = check_safety_limits(sample, config)
        if not is_safe:
            result['end_index'] = i
            result['next_phase'] = 'COMPLETE'
            result['transition_reason'] = violation_msg
            result['safety_violation'] = True
            logger.error(f"TORQUE_HOLD safety violation at index {i}: {violation_msg}")
            return result
        
        # Add sample to processed list
        result['processed_samples'].append(sample)
        result['end_index'] = i + 1
        
        # Check phase transition: elapsed time >= hold_duration_s
        elapsed_s = current_ts - start_ts
        if elapsed_s >= hold_duration_s:
            result['next_phase'] = 'VOLTAGE_DECREASE'
            result['transition_reason'] = f"Hold duration reached: {elapsed_s:.3f} s >= {hold_duration_s} s"
            logger.info(f"TORQUE_HOLD completed at index {i}: {result['transition_reason']}")
            return result
    
    # Data exhausted before hold duration completed
    result['data_exhausted'] = True
    result['next_phase'] = 'COMPLETE'
    result['transition_reason'] = f"Data exhausted during TORQUE_HOLD (processed {len(result['processed_samples'])} samples)"
    logger.warning(result['transition_reason'])
    
    return result


# ─────────────────────────────────────────────────────────────
# VOLTAGE_DECREASE Phase
# ─────────────────────────────────────────────────────────────

def run_voltage_decrease_phase(
    synchronized_data: List[Dict[str, Any]],
    config: Dict[str, Any],
    hold_current_a: float,
    start_index: int = 0
) -> Dict[str, Any]:
    """
    Execute VOLTAGE_DECREASE phase: Decrease PSU voltage while holding current.
    
    Tracks commanded voltage decreasing at voltage_decrease_rate_v_per_s.
    Continues reading all streams.
    Transitions to COMPLETE when voltage <= min_voltage_v or data ends.
    
    Args:
        synchronized_data: List of synchronized samples from synchronize_data()
        config: Parsed test_config.yaml
        hold_current_a: Commanded current to maintain (from TORQUE_HOLD)
        start_index: Index to start processing from
        
    Returns:
        Dict containing:
        - 'processed_samples': List of samples with commanded_current_a/voltage_v added
        - 'end_index': Index where phase ended
        - 'next_phase': Name of next phase ('COMPLETE')
        - 'transition_reason': Why the phase ended
        - 'data_exhausted': True if ended due to running out of data
        - 'safety_violation': True if ended due to safety limit exceeded
    """
    logger.info("VOLTAGE_DECREASE phase: Decreasing PSU voltage...")
    
    # Get phase parameters from YAML
    decrease_params = get_phase_parameters(config, 'VOLTAGE_DECREASE')
    
    if decrease_params is None:
        raise ValueError("VOLTAGE_DECREASE parameters not found in config")
    
    voltage_decrease_rate = decrease_params.get('voltage_decrease_rate_v_per_s', 1.0)
    min_voltage = decrease_params.get('min_voltage_v', 0.0)
    
    # Get initial voltage from power_supply config
    initial_voltage = config['power_supply']['initial_voltage_v']
    
    logger.info(f"  Initial voltage: {initial_voltage:.1f} V")
    logger.info(f"  Decrease rate: {voltage_decrease_rate:.2f} V/s")
    logger.info(f"  Min voltage: {min_voltage:.1f} V")
    logger.info(f"  Hold current: {hold_current_a:.2f} A")
    
    result = {
        'processed_samples': [],
        'end_index': start_index,
        'next_phase': 'COMPLETE',
        'transition_reason': '',
        'data_exhausted': False,
        'safety_violation': False
    }
    
    if start_index >= len(synchronized_data):
        result['transition_reason'] = 'No data to process'
        result['data_exhausted'] = True
        logger.warning("VOLTAGE_DECREASE: No data to process")
        return result
    
    # Get start timestamp for voltage decrease calculation
    start_ts = synchronized_data[start_index]['timestamp_s']
    
    # Process samples
    for i in range(start_index, len(synchronized_data)):
        sample = synchronized_data[i].copy()
        current_ts = sample['timestamp_s']
        
        # Calculate commanded voltage based on elapsed time
        elapsed_s = current_ts - start_ts
        commanded_voltage = max(initial_voltage - (elapsed_s * voltage_decrease_rate), min_voltage)
        
        # Commanded current stays constant
        sample['commanded_current_a'] = hold_current_a
        sample['commanded_voltage_v'] = commanded_voltage
        sample['test_phase'] = 'VOLTAGE_DECREASE'
        
        # Check safety limits first
        is_safe, violation_msg = check_safety_limits(sample, config)
        if not is_safe:
            result['end_index'] = i
            result['transition_reason'] = violation_msg
            result['safety_violation'] = True
            logger.error(f"VOLTAGE_DECREASE safety violation at index {i}: {violation_msg}")
            return result
        
        # Add sample to processed list
        result['processed_samples'].append(sample)
        result['end_index'] = i + 1
        
        # Check phase transition: voltage <= min_voltage
        if commanded_voltage <= min_voltage:
            result['transition_reason'] = f"Min voltage reached: {commanded_voltage:.2f} V <= {min_voltage:.1f} V"
            logger.info(f"VOLTAGE_DECREASE completed at index {i}: {result['transition_reason']}")
            return result
    
    # Data exhausted before min voltage reached
    result['data_exhausted'] = True
    result['transition_reason'] = f"Data exhausted during VOLTAGE_DECREASE (processed {len(result['processed_samples'])} samples)"
    logger.warning(result['transition_reason'])
    
    return result


# ─────────────────────────────────────────────────────────────
# COMPLETE Phase
# ─────────────────────────────────────────────────────────────

def build_output_field_mapping(config: Dict[str, Any]) -> Dict[str, str]:
    """
    Build mapping from output column names to synchronized sample field names.
    
    Reads data_sources from YAML to determine prefixes and field names.
    Synchronized data uses prefixes: motor_*, sensor_*, psu_*
    
    Args:
        config: Parsed test_config.yaml
        
    Returns:
        Dict mapping output column name -> sample field name
    """
    mapping = {}
    
    # Build mapping from data_sources
    data_sources = config.get('data_sources', {})
    
    # Define prefix for each data source
    source_prefixes = {
        'motor': 'motor_',
        'sensor': 'sensor_',
        'power_supply': 'psu_'
    }
    
    for source_name, source_config in data_sources.items():
        prefix = source_prefixes.get(source_name, f'{source_name}_')
        
        # Get columns from CSV format (all our sources use CSV)
        formats = source_config.get('formats', {})
        csv_format = formats.get('csv', {})
        columns = csv_format.get('columns', [])
        
        for col_def in columns:
            col_name = col_def.get('name', '')
            if col_name == 'timestamp_s':
                # Skip timestamp - handled separately
                continue
            
            # The synchronized field name has the prefix
            synced_field = f'{prefix}{col_name}'
            
            # Map various output column names to this field
            # Direct match (e.g., psu_voltage_v -> psu_voltage_v)
            mapping[synced_field] = synced_field
            
            # Also map unprefixed name if it's unique
            # e.g., velocity_rad_s -> motor_velocity_rad_s
            if col_name not in mapping:
                mapping[col_name] = synced_field
            
            # Handle output column name variations
            # e.g., motor_current_a -> motor_measured_current_a
            if source_name == 'motor' and col_name == 'measured_current_a':
                mapping['motor_current_a'] = synced_field
            
            # e.g., torque_nm -> sensor_torque_nm
            if source_name == 'sensor' and col_name == 'torque_nm':
                mapping['torque_nm'] = synced_field
    
    return mapping


def run_complete_phase(
    all_processed_samples: List[Dict[str, Any]],
    config: Dict[str, Any],
    output_path: Path
) -> Dict[str, Any]:
    """
    Execute COMPLETE phase: Write output CSV and display summary.
    
    Flushes all buffers, writes the output CSV with columns from test_config.yaml,
    and displays summary statistics.
    
    Output columns (from test_config.yaml):
    - timestamp_s: Motor timestamp
    - velocity_rad_s: Motor velocity (measured)
    - motor_current_a: Motor current (measured)
    - torque_nm: Torque sensor (measured)
    - psu_voltage_v: PSU voltage (measured from CSV)
    - psu_current_a: PSU current (measured from CSV)
    - commanded_current_a: Current command (computed by state machine)
    - commanded_voltage_v: Voltage command (computed by state machine)
    - test_phase: Current phase name
    
    Args:
        all_processed_samples: List of all samples from all phases
        config: Parsed test_config.yaml
        output_path: Path to write output CSV
        
    Returns:
        Dict containing:
        - 'output_path': Path where CSV was written
        - 'row_count': Number of rows written
        - 'stats': Summary statistics dict
    """
    
    logger.info("COMPLETE phase: Writing output and generating summary...")
    
    # Get output column names from config
    output_columns = config['output']['columns']
    
    # Build field mapping dynamically from data_sources in YAML
    # Maps output column name -> actual field name in synchronized sample
    field_mapping = build_output_field_mapping(config)
    
    result = {
        'output_path': output_path,
        'row_count': 0,
        'stats': {}
    }
    
    if not all_processed_samples:
        logger.warning("COMPLETE: No samples to write")
        return result
    
    # Create output directory if needed
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Write CSV
    logger.info(f"Writing output CSV to {output_path}")
    missing_fields_warned = set()  # Track which fields we've warned about
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=output_columns)
        writer.writeheader()
        
        for sample in all_processed_samples:
            row = {}
            for col in output_columns:
                field_name = field_mapping.get(col, col)
                value = sample.get(field_name)
                
                # Warn once per missing field (except commanded_voltage_v which is phase-specific)
                if value is None and col not in missing_fields_warned:
                    if col != 'commanded_voltage_v':
                        logger.warning(f"Missing field '{field_name}' for output column '{col}' - writing empty values")
                        missing_fields_warned.add(col)
                
                row[col] = value if value is not None else ''
            writer.writerow(row)
        
        result['row_count'] = len(all_processed_samples)
    
    # Log available fields if any were missing (helps debugging)
    if missing_fields_warned:
        logger.warning(f"Available fields in samples: {list(all_processed_samples[0].keys())}")
    
    logger.info(f"Wrote {result['row_count']} rows to {output_path}")
    
    # Compute summary statistics
    stats = compute_output_stats(all_processed_samples, config)
    result['stats'] = stats
    
    # Log summary
    log_complete_summary(stats, output_path, result['row_count'])
    
    logger.info("COMPLETE phase finished")
    return result


def compute_output_stats(samples: List[Dict[str, Any]], config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compute summary statistics for the test run.
    
    Args:
        samples: All processed samples
        config: Parsed test_config.yaml
        
    Returns:
        Dict with statistics:
        - time_span_s: Total test duration
        - phase_counts: Samples per phase
        - max_torque_nm: Maximum torque observed
        - max_current_a: Maximum measured current
        - max_commanded_current_a: Maximum commanded current
    """
    if not samples:
        return {}
    
    # Time span
    first_ts = samples[0].get('timestamp_s', 0)
    last_ts = samples[-1].get('timestamp_s', 0)
    time_span = last_ts - first_ts
    
    # Count samples per phase
    phase_counts = {}
    for sample in samples:
        phase = sample.get('test_phase', 'UNKNOWN')
        phase_counts[phase] = phase_counts.get(phase, 0) + 1
    
    # Find max values
    max_torque = None
    max_measured_current = None
    max_commanded_current = None
    
    for sample in samples:
        torque = sample.get('sensor_torque_nm')
        if torque is not None:
            if max_torque is None or abs(torque) > abs(max_torque):
                max_torque = torque
        
        measured_current = sample.get('motor_measured_current_a')
        if measured_current is not None:
            if max_measured_current is None or abs(measured_current) > abs(max_measured_current):
                max_measured_current = measured_current
        
        commanded_current = sample.get('commanded_current_a')
        if commanded_current is not None:
            if max_commanded_current is None or commanded_current > max_commanded_current:
                max_commanded_current = commanded_current
    
    return {
        'time_span_s': time_span,
        'first_timestamp_s': first_ts,
        'last_timestamp_s': last_ts,
        'total_samples': len(samples),
        'phase_counts': phase_counts,
        'max_torque_nm': max_torque,
        'max_measured_current_a': max_measured_current,
        'max_commanded_current_a': max_commanded_current
    }


def log_complete_summary(stats: Dict[str, Any], output_path: Path, row_count: int) -> None:
    """
    Log summary statistics for the test run.
    
    Args:
        stats: Statistics from compute_output_stats
        output_path: Path where CSV was written
        row_count: Number of rows written
    """
    logger.info("=" * 60)
    logger.info("COMPLETE Phase Summary")
    logger.info("=" * 60)
    
    logger.info(f"Output file: {output_path}")
    logger.info(f"Total rows: {row_count}")
    
    if stats:
        logger.info(f"Time span: {stats.get('time_span_s', 0):.3f} s")
        logger.info(f"  First timestamp: {stats.get('first_timestamp_s', 0):.3f} s")
        logger.info(f"  Last timestamp: {stats.get('last_timestamp_s', 0):.3f} s")
        
        logger.info("Samples per phase:")
        for phase, count in stats.get('phase_counts', {}).items():
            logger.info(f"  {phase}: {count}")
        
        max_torque = stats.get('max_torque_nm')
        if max_torque is not None:
            logger.info(f"Max torque: {max_torque:.2f} Nm")
        
        max_measured = stats.get('max_measured_current_a')
        if max_measured is not None:
            logger.info(f"Max measured current: {max_measured:.2f} A")
        
        max_commanded = stats.get('max_commanded_current_a')
        if max_commanded is not None:
            logger.info(f"Max commanded current: {max_commanded:.2f} A")
    
    logger.info("=" * 60)
