"""
Automation module - State machine, synchronization, and safety logic.

This module contains:
- state_machine: YAML-driven test phase management (pure functions)
- synchronization: Multi-rate data alignment (nearest-prior join)
- safety: Threshold monitoring and abort conditions
"""

from .state_machine import (
    run_setup_phase,
    run_current_ramp_phase,
    run_torque_hold_phase,
    run_voltage_decrease_phase,
    run_complete_phase,
    get_phase_parameters
)
from .synchronization import synchronize_data, nearest_prior_index, find_timestamp_key
from .safety import check_safety_limits, check_phase_transition_current_ramp, get_safety_thresholds

__all__ = [
    # State machine phases
    'run_setup_phase',
    'run_current_ramp_phase',
    'run_torque_hold_phase',
    'run_voltage_decrease_phase',
    'run_complete_phase',
    'get_phase_parameters',
    # Synchronization
    'synchronize_data',
    'nearest_prior_index',
    'find_timestamp_key',
    # Safety
    'check_safety_limits',
    'check_phase_transition_current_ramp',
    'get_safety_thresholds',
]
