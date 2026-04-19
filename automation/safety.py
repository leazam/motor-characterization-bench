"""
Safety monitoring and threshold checks.

Monitors data against safety thresholds defined in test_config.yaml.
Triggers abort conditions when limits are exceeded.

Implemented as pure functions - no classes.
"""

import logging
from typing import Dict, Any, List, Optional, Tuple

logger = logging.getLogger(__name__)


def check_safety_limits(
    sample: Dict[str, Any],
    config: Dict[str, Any]
) -> Tuple[bool, Optional[str]]:
    """
    Check if a synchronized sample violates any safety limits.
    
    Safety limits (from PDF):
    - Torque: |torque| must not exceed max_torque_nm
    - Current: COMMANDED current must not exceed max_current_a
    
    Args:
        sample: Synchronized data sample with sensor_torque_nm, commanded_current_a, etc.
        config: Parsed test_config.yaml dictionary
        
    Returns:
        Tuple of (is_safe, violation_message):
        - is_safe: True if within limits, False if violation
        - violation_message: Description of violation, or None if safe
    """
    safety = config['test']['safety']
    max_torque = safety['max_torque_nm']
    max_current = safety['max_current_a']
    
    # Check torque limit (use absolute value)
    torque = sample.get('sensor_torque_nm')
    if torque is not None and abs(torque) > max_torque:
        msg = f"SAFETY VIOLATION: Torque |{torque:.2f}| Nm exceeds limit {max_torque} Nm"
        logger.error(msg)
        return False, msg
    
    # Check COMMANDED current limit (not measured current!)
    # Commanded current is what we tell the motor to do - must never exceed limit
    commanded_current = sample.get('commanded_current_a')
    if commanded_current is not None and commanded_current > max_current:
        msg = f"SAFETY VIOLATION: Commanded current {commanded_current:.2f} A exceeds limit {max_current} A"
        logger.error(msg)
        return False, msg
    
    return True, None


def check_phase_transition_current_ramp(
    sample: Dict[str, Any],
    config: Dict[str, Any]
) -> Tuple[bool, str]:
    """
    Check if CURRENT_RAMP phase should transition to TORQUE_HOLD.
    
    Transition when |torque| >= target_torque OR |measured_current| >= max_current.
    
    Note: This checks MEASURED current (motor response), not commanded current.
    Safety check uses commanded current, phase transition uses measured current.
    
    Args:
        sample: Synchronized data sample
        config: Parsed test_config.yaml dictionary
        
    Returns:
        Tuple of (should_transition, reason):
        - should_transition: True if transition condition met
        - reason: Description of why transition should occur
    """
    # Get CURRENT_RAMP parameters from YAML
    phases = config['test']['phases']
    current_ramp_params = None
    for phase in phases:
        if phase['name'] == 'CURRENT_RAMP':
            current_ramp_params = phase.get('parameters', {})
            break
    
    if current_ramp_params is None:
        logger.warning("CURRENT_RAMP parameters not found in config")
        return False, ""
    
    target_torque = current_ramp_params.get('target_torque_nm', 150.0)
    max_current = current_ramp_params.get('max_current_a', 34.0)
    
    # Check torque condition
    torque = sample.get('sensor_torque_nm')
    if torque is not None and abs(torque) >= target_torque:
        reason = f"Target torque reached: |{torque:.2f}| >= {target_torque} Nm"
        logger.info(f"CURRENT_RAMP transition: {reason}")
        return True, reason
    
    # Check MEASURED current condition (what motor actually draws)
    measured_current = sample.get('motor_measured_current_a') or sample.get('motor_measured_current')
    if measured_current is not None and abs(measured_current) >= max_current:
        reason = f"Max measured current reached: |{measured_current:.2f}| >= {max_current} A"
        logger.info(f"CURRENT_RAMP transition: {reason}")
        return True, reason
    
    return False, ""


def get_safety_thresholds(config: Dict[str, Any]) -> Dict[str, float]:
    """
    Extract safety thresholds from config.
    
    Args:
        config: Parsed test_config.yaml dictionary
        
    Returns:
        Dict with max_torque_nm and max_current_a
    """
    safety = config['test']['safety']
    return {
        'max_torque_nm': safety['max_torque_nm'],
        'max_current_a': safety['max_current_a']
    }
