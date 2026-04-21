#!/usr/bin/python3.13
"""
Motor Characterization Test Bench - Main Entry Point

Requires Python 3.6+

Replays and processes data from a motor characterization test bench.
Synchronizes data from three instruments (motor controller, torque sensor, PSU)
and executes a YAML-driven test state machine.
"""

import argparse
import sys
import logging
import yaml
from pathlib import Path

# Absolute path to the directory containing main.py
SCRIPT_DIR = Path(__file__).resolve().parent

from automation.state_machine import run_setup_phase, run_current_ramp_phase, run_torque_hold_phase, run_voltage_decrease_phase, run_complete_phase
from automation.synchronization import synchronize_data


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Motor Characterization Test Bench - Data Replay & Processing",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # Configuration files
    parser.add_argument(
        "-mp", "--motor-protocol", type=Path, required=False, default=SCRIPT_DIR / "config" / "motor_protocol.yaml",
        help="Path to motor_protocol.yaml"
    )
    parser.add_argument(
        "-tc", "--test-config", type=Path, required=False, default=SCRIPT_DIR / "config" / "test_config.yaml",
        help="Path to test_config.yaml (test parameters and phases)"
    )
    
    # Data files
    parser.add_argument(
        "-md", "--motor-data", type=Path, required=False, default=None,
        help="Path to motor data file (.bin or .csv) - required for CLI mode"
    )
    parser.add_argument(
        "-sd", "--sensor-data", type=Path, required=False, default=SCRIPT_DIR / "data" / "test_sensor_4800hz.csv",
        help="Path to torque sensor CSV file"
    )
    parser.add_argument(
        "-psu", "--psu-data", type=Path, required=False, default=SCRIPT_DIR / "data" / "test_psu_10hz.csv",
        help="Path to power supply CSV file"
    )
    
    # Output
    parser.add_argument(
        "-o", "--output", type=Path, required=False, default=SCRIPT_DIR / "output" / "output.csv",
        help="Path to output CSV file (default: output/output.csv)"
    )

    parser.add_argument(
        "-ng", "--not-gui", action="store_true",
        help="Run the application without GUI"
    )
    
    # Logging options
    parser.add_argument(
        "-l", "--log-file", type=Path, required=False, default=SCRIPT_DIR / "logs" / "run.log",
        help="Path to log file (default: logs/run.log)"
    )
    parser.add_argument(
        "-v", "--verbose", type=int, choices=[0, 1, 2], default=0,
        help="Verbosity level: 0=warnings/errors (default), 1=info, 2=debug"
    )
    
    return parser.parse_args()




def configure_logging(log_file: Path, verbosity: int = 0) -> None:
    """
    Configure logging to output to terminal and to a file.
    
    Terminal output respects verbosity level.
    File output always logs everything (DEBUG level).
    
    Args:
        log_file: Path to log file.
        verbosity: Terminal verbosity: 0=WARNING (default), 1=INFO, 2=DEBUG
    """
    # Map verbosity level to logging level for terminal
    level_map = {
        0: logging.WARNING,
        1: logging.INFO,
        2: logging.DEBUG
    }
    terminal_level = level_map.get(verbosity, logging.WARNING)
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Get root logger - set to DEBUG so all messages flow through
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    
    # Clear any existing handlers
    root_logger.handlers.clear()
    
    # Terminal handler - uses verbosity level
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(terminal_level)
    console_handler.setFormatter(logging.Formatter(log_format))
    root_logger.addHandler(console_handler)
    
    # File handler - always logs everything (DEBUG)
    # Create parent directories if needed
    log_file.parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)  # Always log everything to file
    file_handler.setFormatter(logging.Formatter(log_format))
    root_logger.addHandler(file_handler)


def validate_file_exists(filepath: Path, description: str) -> None:
    """
    Validate that a file exists.
    
    Args:
        filepath: Path to check
        description: Human-readable description for error message
        
    Raises:
        FileNotFoundError: If file does not exist
    """
    if not filepath.exists():
        raise FileNotFoundError(f"{description} not found: {filepath}")
    if not filepath.is_file():
        raise FileNotFoundError(f"{description} is not a file: {filepath}")


def load_yaml_config(filepath: Path) -> dict:
    """
    Load and parse a YAML configuration file.
    
    Args:
        filepath: Path to YAML file
        
    Returns:
        Parsed YAML as dictionary
        
    Raises:
        yaml.YAMLError: If YAML parsing fails
    """
    with open(filepath, 'r', encoding='utf-8') as f:    #TODO: read about utf-8 encoding
        return yaml.safe_load(f)


def main() -> int:
    # Parse arguments first (needed for logging config)
    args = parse_arguments()
    
    # If GUI mode (default), launch the GUI
    if not args.not_gui:
        from ui.app import run_gui
        run_gui()
        return 0
    
    # CLI mode: --motor-data is required
    if args.motor_data is None:
        print("Error: --motor-data is required in CLI mode (--not-gui)")
        return 1
    
    # Configure logging (terminal + file, with verbosity level)
    configure_logging(log_file=args.log_file, verbosity=args.verbose)
    logger = logging.getLogger(__name__)
    
    logger.info(f"Logging to file: {args.log_file.resolve()}")
    
    # Validate all input files exist
    try:
        validate_file_exists(args.motor_protocol, "Motor protocol YAML")
        validate_file_exists(args.test_config, "Test config YAML")
        validate_file_exists(args.motor_data, "Motor data file")
        validate_file_exists(args.sensor_data, "Sensor data file")
        validate_file_exists(args.psu_data, "PSU data file")
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        return 1
    
    # Load YAML configurations
    try:
        motor_protocol = load_yaml_config(args.motor_protocol)
        test_config = load_yaml_config(args.test_config)
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML: {e}")
        return 1
    
    # Log loaded configuration summary
    logger.info(f"Loaded motor protocol: {motor_protocol['protocol']['name']} "
                f"v{motor_protocol['protocol']['version']}")
    logger.info(f"Loaded test config: {test_config['test']['name']}")
    logger.info(f"  Phases: {[p['name'] for p in test_config['test']['phases']]}")
    logger.info(f"  Safety limits: max_torque={test_config['test']['safety']['max_torque_nm']} Nm, "
                f"max_current={test_config['test']['safety']['max_current_a']} A")
    
    logger.info(f"Data files:")
    logger.info(f"  Motor:  {args.motor_data}")
    logger.info(f"  Sensor: {args.sensor_data}")
    logger.info(f"  PSU:    {args.psu_data}")
    logger.info(f"Output: {args.output}")
    
    # Run SETUP phase: Load and validate all data files
    try:
        setup_result = run_setup_phase(
            motor_data_path=args.motor_data,
            sensor_data_path=args.sensor_data,
            psu_data_path=args.psu_data,
            test_config=test_config,
            motor_protocol=motor_protocol
        )
    except ValueError as e:
        logger.error(f"SETUP phase failed: {e}")
        return 1
    
    # Extract loaded data for subsequent phases
    motor_data = setup_result['motor_data']
    sensor_data = setup_result['sensor_data']
    psu_data = setup_result['psu_data']
    
    logger.info(f"Loaded {len(motor_data)} motor, {len(sensor_data)} sensor, {len(psu_data)} PSU samples")
    
    # Synchronize data streams (nearest-prior join)
    try:
        synchronized_data, sync_stats = synchronize_data(
            motor_data=motor_data,
            sensor_data=sensor_data,
            psu_data=psu_data,
            config=test_config
        )
    except ValueError as e:
        logger.error(f"Synchronization failed: {e}")
        return 1
    
    # Accumulate all processed samples across phases
    all_processed_samples = []
    current_index = 0
    final_phase = 'SETUP'
    last_result = None  # Track the last phase result for summary
    
    # Run CURRENT_RAMP phase
    logger.info("=" * 60)
    try:
        ramp_result = run_current_ramp_phase(
            synchronized_data=synchronized_data,
            config=test_config,
            start_index=current_index
        )
    except ValueError as e:
        logger.error(f"CURRENT_RAMP phase failed: {e}")
        return 1
    all_processed_samples.extend(ramp_result['processed_samples'])
    current_index = ramp_result['end_index']
    final_phase = 'CURRENT_RAMP'
    last_result = ramp_result
    
    # Check for safety violation or data exhaustion
    if ramp_result['safety_violation']:
        logger.error(f"Test stopped due to safety violation: {ramp_result['transition_reason']}")
        final_phase = 'COMPLETE (safety violation)'
    elif ramp_result['data_exhausted']:
        logger.warning(f"Data exhausted during CURRENT_RAMP: {ramp_result['transition_reason']}")
        final_phase = 'CURRENT_RAMP (data exhausted)'
    else:
        logger.info(f"CURRENT_RAMP completed: {ramp_result['transition_reason']}")
        
        # Get the final commanded current from CURRENT_RAMP
        hold_current = ramp_result['processed_samples'][-1]['commanded_current_a']
        
        # Run TORQUE_HOLD phase
        logger.info("=" * 60)
        try:
            hold_result = run_torque_hold_phase(
                synchronized_data=synchronized_data,
                config=test_config,
                hold_current_a=hold_current,
                start_index=current_index
            )
        except ValueError as e:
            logger.error(f"TORQUE_HOLD phase failed: {e}")
            return 1
        all_processed_samples.extend(hold_result['processed_samples'])
        current_index = hold_result['end_index']
        final_phase = 'TORQUE_HOLD'
        last_result = hold_result
        
        # Check for safety violation or data exhaustion
        if hold_result['safety_violation']:
            logger.error(f"Test stopped due to safety violation: {hold_result['transition_reason']}")
            final_phase = 'COMPLETE (safety violation)'
        elif hold_result['data_exhausted']:
            logger.warning(f"Data exhausted during TORQUE_HOLD: {hold_result['transition_reason']}")
            final_phase = 'TORQUE_HOLD (data exhausted)'
        else:
            logger.info(f"TORQUE_HOLD completed: {hold_result['transition_reason']}")
            
            # Run VOLTAGE_DECREASE phase
            logger.info("=" * 60)
            try:
                decrease_result = run_voltage_decrease_phase(
                    synchronized_data=synchronized_data,
                    config=test_config,
                    hold_current_a=hold_current,
                    start_index=current_index
                )
            except ValueError as e:
                logger.error(f"VOLTAGE_DECREASE phase failed: {e}")
                return 1
            all_processed_samples.extend(decrease_result['processed_samples'])
            current_index = decrease_result['end_index']
            final_phase = 'VOLTAGE_DECREASE'
            last_result = decrease_result
            
            # Check for safety violation or data exhaustion
            if decrease_result['safety_violation']:
                logger.error(f"Test stopped due to safety violation: {decrease_result['transition_reason']}")
                final_phase = 'COMPLETE (safety violation)'
            elif decrease_result['data_exhausted']:
                logger.warning(f"Data exhausted during VOLTAGE_DECREASE: {decrease_result['transition_reason']}")
                final_phase = 'VOLTAGE_DECREASE (data exhausted)'
            else:
                logger.info(f"VOLTAGE_DECREASE completed: {decrease_result['transition_reason']}")
                final_phase = 'COMPLETE'
    
    # Run COMPLETE phase (generate output log)
    logger.info("=" * 60)
    try:
        complete_result = run_complete_phase(
            all_processed_samples=all_processed_samples,
            config=test_config,
            output_path=args.output
        )
    except Exception as e:
        logger.error(f"COMPLETE phase failed: {e}")
        return 1
    
    # Summary
    logger.info("=" * 60)
    logger.info("Test Run Summary")
    logger.info("=" * 60)
    logger.info(f"  Final phase: {final_phase}")
    logger.info(f"  Processed samples: {len(all_processed_samples)}")
    logger.info(f"  Output file: {complete_result['output_path']}")
    if last_result:
        logger.info(f"  Data exhausted: {last_result['data_exhausted']}")
        logger.info(f"  Safety violation: {last_result['safety_violation']}")
    logger.info("=" * 60)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
